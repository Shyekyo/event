package ideal

import java.util.Properties

import ideal.constants.Constants
import ideal.util.DBUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{ByteType, Decimal, DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxiaofan on 2019/6/24.
  */
object Rep_total_account_commit {
  val _logging = Logger.getLogger(Rep_total_account_commit.getClass)

  val url = Constants.ORACLE_URL
  val TOTABLE = "REP_TMP"
  var bureauId = 1
  var refNo =1

  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.WARN)
    if(args.length==2){
      bureauId =args(0).toInt
      refNo = args(0).toInt
    }
    val spark = SparkSession.builder().
      appName("xsql").
      master(Constants.SPARK_APP_MASTER_LOCAL).
      config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse")
      //config("spark.sql.warehouse.dir", prefix2+"spark-warehouse").
      .getOrCreate()
    import spark.implicits._

    val props = getOracleProps()
    val dfList = new ArrayBuffer[DataFrame]
    val account_type = "ACCOUNT_TYPE"
    val account_type_df = loadDataFromOracle(spark,url,account_type,props)
    account_type_df.createOrReplaceTempView("account_type")
    dfList += account_type_df

    val acct_item = "ACCT_ITEM"
    val acct_item_df = loadDataFromOracle(spark,url,acct_item,props)
    acct_item_df.createOrReplaceTempView("acct_item")
    dfList += acct_item_df

    val acct_item_type = "ACCT_ITEM_TYPE"
    val acct_item_type_df = loadDataFromOracle(spark,url,acct_item_type,props)
    acct_item_type_df.createOrReplaceTempView("acct_item_type")
    dfList += acct_item_type_df

    val amount = "AMOUNT"
    val amount_df = loadDataFromOracle(spark,url,amount,props)
    amount_df.createOrReplaceTempView("amount")
    dfList += amount_df

    val billing_cycle = "BILLING_CYCLE"
    val billing_cycle_df = loadDataFromOracle(spark,url,billing_cycle,props)
    billing_cycle_df.createOrReplaceTempView("billing_cycle")
    dfList += billing_cycle_df

    val gen_item = "GEN_ITEM"
    val gen_item_df = loadDataFromOracle(spark,url,gen_item,props)
    gen_item_df.createOrReplaceTempView("gen_item")
    dfList += gen_item_df

    val stt_object = "STT_OBJECT"
    val stt_object_df = loadDataFromOracle(spark,url,stt_object,props)
    stt_object_df.createOrReplaceTempView("stt_object")
    dfList += stt_object_df

    showHead(dfList)

    val schema_ad = StructType(
        StructField("GEN_ID",DecimalType(9,0) , true)::
        StructField("REF_NO", StringType, true)::
        StructField("CUST_NAME", StringType, true)::
        StructField("ACCT_ITEM_TYPE_ID", DecimalType(9,0), true)::
        StructField("NAME", StringType, true)::
        StructField("CYCLE_BEGIN_DATE", DecimalType(8,0), true)::
        StructField("CYCLE_END_DATE", DecimalType(8,0), true)::
        StructField("AMOUNT",DecimalType(13,4), true)::
        StructField("CURRENCY",StringType , true)::
        StructField("PAY_FLAG", StringType, true)::
        StructField("START_DATE", DecimalType(8,0), true)::
        StructField("END_DATE", DecimalType(8,0), true)::
        StructField("SORT_ID", IntegerType, true)::
        StructField("ACCT_NAME", StringType, true)::
        StructField("ACCT_DATE", DecimalType(8,0), true)::
        Nil
    )

    val sql = new StringBuilder()
    sql.append("select ")
    .append("g.gen_id,g.ref_no,stt.cust_name,ait.acct_item_type_id,ait.name,bc.cycle_begin_date,")
    .append("bc.cycle_end_date,a.amount,a.currency,ait.pay_flag,g.start_date,g.end_date,")
    .append("case when (case when (case when instr(ait.name, '调整')>0 then 1 else -1 end)==1 then ai.comments else at.acct_type end)=='TELEPHONE' then 1 else ")
    .append("(case when (case when (case when instr(ait.name, '调整')>0 then 1 else -1 end)==1 then ai.comments else at.acct_type end)=='TELX' then 2 else ")
    .append("(case when (case when (case when instr(ait.name, '调整')>0 then 1 else -1 end)==1 then ai.comments else at.acct_type end)=='TELEGRAM' then 3 else ")
    .append("(case when (case when (case when instr(ait.name, '调整')>0 then 1 else -1 end)==1 then ai.comments else at.acct_type end)=='COMMITMENT ADJUST' then 5 else ")
    .append("(case when (case when (case when instr(ait.name, '调整')>0 then 1 else -1 end)==1 then ai.comments else at.acct_type end)==ai.comments then 6 else 4 end) end) end) end) end sort_id,")
    .append("(case when (case when instr(ait.name,'调整')==1 then 1 else -1 end)==1 then ai.comments else at.acct_type end) acct_name,")
    .append("g.acct_date acct_date")
    .append(" from gen_item g,acct_item_type ait,acct_item ai,amount a, stt_object stt,billing_cycle bc,account_type at ")
    .append("where ")
    .append("a.account_item_id = ai.acct_item_id ")
    .append("and ai.acct_item_type_id = ait.acct_item_type_id ")
    .append("and ai.gen_id = g.gen_id ")
    .append("and g.cust_id = stt.cust_id ")
    .append("and at.account_type_id = case when ait.account_type_id=59 then 47 when ait.account_type_id=60 then 48 when ait.account_type_id=61 then 49 when ait.account_type_id=71 then 69 when ait.account_type_id=70 then 68 else ait.account_type_id end ")
    .append("and ai.billing_cycle_id = bc.billing_cycle_id ")
    .append("and g.bureau_id = ai.bureau_id ")
    .append("and g.bureau_id ="+bureauId)
    .append(" and g.ref_no ="+refNo)

    val frame1 = spark.sql(sql.toString())

    var rep_tmp = spark.createDataFrame(frame1.rdd,schema_ad)
    rep_tmp.show()
    writeDataIntoOrcale(rep_tmp,TOTABLE)

    sql.clear()
    sql.append("select null gen_id,gen_item.ref_no,min(stt_object.cust_name) cust_name, acct_item_type.acct_item_type_id,")
     .append("'-1' name,1 cycle_begin_date,2 cycle_end_date,sum(amount.amount) / 3.061 amount,'SDR' currency,acct_item_type.pay_flag,")
     .append("null start_date,null end_date,8 sort_id,'' acct_name,min(gen_item.acct_date) acct_date ")
     .append("from gen_item,acct_item_type,acct_item,amount,stt_object ")
     .append("where gen_item.gen_id = acct_item.gen_id ")
     .append("and acct_item.acct_item_type_id = acct_item_type.acct_item_type_id ")
     .append("and acct_item.acct_item_id = amount.account_item_id ")
     .append("and gen_item.cust_id = stt_object.cust_id ")
     .append("and amount.currency = 'GF' ")
     .append("and gen_item.bureau_id = acct_item.bureau_id ")
     .append("and gen_item.bureau_id ="+ bureauId)
     .append(" and gen_item.ref_no ="+ refNo)
     .append(" group by gen_item.ref_no, acct_item_type.pay_flag, acct_item_type.acct_item_type_id")

    val frame2 = spark.sql(sql.toString())
    rep_tmp = spark.createDataFrame(frame2.rdd,schema_ad)
    rep_tmp.show()
    writeDataIntoOrcale(rep_tmp,TOTABLE)
//===========================================================================
   sql.clear()
   sql.append("select count(*) as count from ")
    .append(TOTABLE)
    .append(" where pay_flag = 'C' and currency != 'USD'")
   val array = DBUtil.queryOra(sql.toString())
   var count = 0
   if(array.length==1){
     for(line <- array){
       for(num <- line){
         println(count)
         count=num.toInt
       }
     }
   }
   if(count==0){
     sql.clear()
     sql.append("insert into ")
       .append(TOTABLE)
       .append(" select gen_id,ref_no,")
       .append("cust_name,acct_item_type_id,name,cycle_begin_date,cycle_end_date,")
       .append("null,currency,'C',start_date,end_date,sort_id,acct_name,acct_date ")
       .append("from ")
       .append(TOTABLE)
       .append(" where pay_flag = 'D' and 'currency' != 'USD'")
     DBUtil.addSQL(sql.toString())
   }
//-----------------------------------------------------------------
    sql.clear()
    sql.append("select count(*) as count from ")
      .append(TOTABLE)
      .append(" where pay_flag = 'D' and currency != 'USD'")
    val array1 = DBUtil.queryOra(sql.toString())
    var count1 = 0
    if(array1.length==1){
      for(line <- array){
        for(num <- line){
          count1=num.toInt
        }
      }
    }
    if(count1==0){
      sql.clear()
      sql.append("insert into ")
        .append(TOTABLE)
        .append(" select gen_id,ref_no,")
        .append("cust_name,acct_item_type_id,name,cycle_begin_date,cycle_end_date,")
        .append("null,currency,'D',start_date,end_date,sort_id,acct_name,acct_date ")
        .append("from ")
        .append(TOTABLE)
        .append(" where pay_flag = 'D' and currency != 'USD'")
      DBUtil.addSQL(sql.toString())
    }
    //---------------------------------------------------------
    sql.clear()
    sql.append("select count(*) as count from ")
      .append(TOTABLE)
      .append(" where pay_flag = 'C' and currency = 'USD'")
    val array2 = DBUtil.queryOra(sql.toString())
    var count2 = 0
    if(array2.length==1){
      for(line <- array){
        for(num <- line){
          count2=num.toInt
        }
      }
    }
    if(count2==0){
      sql.clear()
      sql.append("insert into ")
        .append(TOTABLE)
        .append(" select gen_id,ref_no,")
        .append("cust_name,acct_item_type_id,name,cycle_begin_date,cycle_end_date,")
        .append("null,'USD','D',start_date,end_date,sort_id,acct_name,acct_date ")
        .append("from ")
        .append(TOTABLE )
        .append(" where pay_flag = 'D' and currency = 'USD'")
      DBUtil.addSQL(sql.toString())
    }
 //---------------------------------------------------------------
    sql.clear()
    sql.append("select count(*) as count from ")
      .append(TOTABLE)
      .append(" where pay_flag = 'D' and currency = 'USD'")
    val array3 = DBUtil.queryOra(sql.toString())
    var count3 = 0
    if(array3.length==1){
      for(line <- array){
        for(num <- line){
          count3=num.toInt
        }
      }
    }
    if(count3==0){
      sql.clear()
      sql.append("insert into ")
        .append(TOTABLE)
        .append(" select gen_id,ref_no,")
        .append("cust_name,acct_item_type_id,name,cycle_begin_date,cycle_end_date,")
        .append("null,'USD','D',start_date,end_date,sort_id,acct_name,acct_date ")
        .append("from ")
        .append(TOTABLE)
        .append(" where pay_flag = 'C' and currency = 'USD'")
      DBUtil.addSQL(sql.toString())
    }
    //-------------------------------------------------------------------
    spark.stop()
  }
  private def getOracleProps():Properties={
    val props = new Properties()
    props.put("user", Constants.ORACLE_USER)
    props.put("password", Constants.ORACLE_PASSWORD)
    props.put("driver", Constants.ORACLE_JDBC_DRIVER)
    props
  }

  private def writeDataIntoOrcale(df: DataFrame, TableName: String): Unit = {
    val props = getOracleProps()
    df.write.mode(SaveMode.Append)
      .jdbc(
        Constants.ORACLE_URL,
        TableName,
        props
      )
    _logging.info("save table "+TableName+" is ok !")
  }

  private def showHead(dfs:ArrayBuffer[DataFrame],num:Int=20):Unit = {
    for(df <- dfs){
      df.show(num)
    }
  }

  private def loadDataFromOracle(spark:SparkSession,url:String,tableName:String,props:Properties):DataFrame={
    spark.read.format("jdbc").jdbc(url,tableName,props)
    /*.option("url",url)
     .option("driver","oracle.jdbc.driver.OracleDriver")
     .option("dbtable",table)
     .option("user","ivoss")
     .option("password","Orivoss_m_204").load()*/
  }
}