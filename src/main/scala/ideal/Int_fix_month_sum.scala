package ideal

import ideal.Rep_total_account_commit1.{bureauId, refNo}
import ideal.constants.Constants
import ideal.util.{DBUtil, DateUtil, SparkUtil}
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.DataEncryptionKeyProtoOrBuilder
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, StructField, StructType}

/**
  * Created by zhangxiaofan on 2019/6/27.
  */
object Int_fix_month_sum {
  val _logging = Logger.getLogger(Int_fix_month_sum.getClass)
  var p_acct_data_class=31
  var p_cycle_id = 201806000
  var p_bureau_id = 4

  def main(args: Array[String]): Unit = {
    if (args.length == 2) {
      p_acct_data_class = args(0).toInt
      p_cycle_id = args(1).toInt
      p_bureau_id = args(2).toInt
    }

    //Logger.getLogger("org").setLevel(Level.WARN)
    if(args.length==2){
      bureauId =args(0).toInt
      refNo = args(0).toInt
    }
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      master(Constants.SPARK_APP_MASTER_LOCAL).
      config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse")
      //config("spark.sql.warehouse.dir", prefix2+"spark-warehouse").
      .getOrCreate()
    import spark.implicits._

    var v_condition : String = null
    var v_first_date : String = null
    var v_last_date : String = null

    _logging.info("1 step...................")
    val v_acct_data_class = p_acct_data_class.toString
    val v_bureau_id =p_bureau_id.toString
    val v_file_type =p_bureau_id.toString
    val v_bill_id = p_cycle_id;
    val v_bill_id_char = p_cycle_id.toString
    var v_month = v_bill_id_char.substring(0,6)
    val v_start_time = DateUtil.getToday()

    if  (p_bureau_id==1 || p_bureau_id==3){
      v_condition = s" route_id in (1,3) and settle_month = ${p_cycle_id}"
    }else{
      v_condition = s" route_id=${p_bureau_id} and settle_month = ${p_cycle_id}"
    }
    _logging.info(v_condition+"...................")

    val sql = new StringBuilder()
    sql.append("SELECT ")
        .append("begin_date,end_date ")
        .append("from acct_billing_cycle ")
        .append(s"where BILLING_CYCLE_ID = ${p_cycle_id}")
    val begin_end_time_rs : Array[Array[String]]= DBUtil.queryOra(sql.toString,205)

    for(row <- begin_end_time_rs){
      v_first_date = row(0)
      v_last_date = row(1)
    }
    _logging.info("2 step...................")

    val tail = "T"
    val CdrDaySumBur = s"cdr_day_sum_bur${v_bureau_id}_${p_cycle_id}${tail}"
    _logging.info(CdrDaySumBur)

    if(!DBUtil.tableExists(CdrDaySumBur)){
      val sql = getCreate_CdrDaySumBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,CdrDaySumBur,destation=205)
    }else{
      DBUtil.clearTable(CdrDaySumBur,205)
    }

    val CdrHourSumBur = s"cdr_hour_sum_bur${v_bureau_id}_${p_cycle_id}${tail}"
    if(!DBUtil.tableExists(CdrHourSumBur)){
      val sql = getCreate_CdrHourSumBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,CdrHourSumBur,destation=205)
    }else{
      DBUtil.clearTable(CdrHourSumBur,205)
    }
    _logging.info("4 step...................")

    val DaySumBur = s"day_sum_bur${v_bureau_id}_${p_cycle_id}${tail}"
    if(!DBUtil.tableExists(DaySumBur)){
      val sql = getCreate_DaySumBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,DaySumBur,destation=205)
    }else{
      DBUtil.clearTable(DaySumBur,205)
    }

    _logging.info("5 step...................")
    val DaySumNobillBur = s"day_sum_nobill_bur${v_bureau_id}_$p_cycle_id${tail}"
    if(!DBUtil.tableExists(DaySumNobillBur)){
      val sql = getCreate_DaySumNobillBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,DaySumNobillBur,destation=205)
    }else{
      DBUtil.clearTable(DaySumNobillBur,205)
    }

    _logging.info("6 step...................")
    val HourSumBur = s"hour_sum_bur${v_bureau_id}_$p_cycle_id${tail}"
    if(!DBUtil.tableExists(HourSumBur)){
      val sql = getCreate_HourSumBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,HourSumBur,destation=205)
    }else{
      DBUtil.clearTable(HourSumBur,205)
    }

    _logging.info("7 step...................")
    val HourSumNobillBur = s"hour_sum_nobill_bur${v_bureau_id}_$p_cycle_id${tail}"
    if(!DBUtil.tableExists(HourSumNobillBur)){
      val sql = getCreate_HourSumNobillBur_SQL(v_bureau_id,p_cycle_id)
      DBUtil.createTable(sql,HourSumNobillBur,destation=205)
    }else{
      DBUtil.clearTable(HourSumNobillBur,205)
    }

    _logging.info("8 step...................")

    val groupedCdr = s"grouped_cdr_$v_month"
    val props = SparkUtil.getOracleCtgirmProps()
    val groupedCdrdf = SparkUtil.loadDataFromOracle(spark,Constants.ORACLE_CTGIRM_URL,groupedCdr,props)
    groupedCdrdf.createOrReplaceTempView(groupedCdr)
    groupedCdrdf.show()

    val schema_day = StructType(
      StructField("CALL_YEAR"          ,DecimalType(4,0) ,false)::
        StructField("CALL_MONTH"         ,DecimalType(2,0) ,false)::
        StructField("CALL_DAY"           ,DecimalType(2,0) ,false)::
        StructField("CALLER_COUNTRY_ID"  ,DecimalType(5,0) ,false)::
        StructField("CALLER_PROVINCE_ID" ,DecimalType(5,0) ,false)::
        StructField("CALLER_ZONE"        ,DecimalType(5,0) ,false)::
        StructField("CALLER_CARRIER_ID"  ,DecimalType(5,0) ,false)::
        StructField("CALLER_TYPE"        ,StringType       ,false)::
        StructField("CALLEE_TYPE"        ,StringType       ,false)::
        StructField("CALLEE_CARRIER_ID"  ,DecimalType(5,0) ,false)::
        StructField("CALLEE_ZONE"        ,DecimalType(5,0) ,false)::
        StructField("CALLEE_PROVINCE_ID" ,DecimalType(5,0) ,false)::
        StructField("CALLEE_COUNTRY_ID"  ,DecimalType(5,0) ,false)::
        StructField("TRANS_CO"           ,DecimalType(5,0) ,true    )::
        StructField("SERVICE_TYPE"       ,DecimalType(3,0) ,false)::
        StructField("DIRECTION"          ,DecimalType(2,0) ,false)::
        StructField("ROUTE_TYPE"         ,StringType       ,false)::
        StructField("SWITCH_ID"          ,DecimalType(5,0) ,false)::
        StructField("COUNTS"             ,DecimalType(18,0),true    )::
        StructField("SECONDS"            ,DecimalType(18,0),true    )::
        StructField("BILL_MINUTE"        ,DecimalType(18,0),true    )::
        StructField("SETTLE_MIN"         ,DecimalType(18,0),true    )::
        StructField("INTRUNK_CARRIER_ID" ,DecimalType(5,0) ,false)::
        StructField("OUTTRUNK_CARRIER_ID",DecimalType(5,0) ,false)::
        StructField("ROUTE_ID"           ,DecimalType(10,0),false)::
        StructField("PRE_FEE"            ,DecimalType(18,0),true    )::
        StructField("FEE"                ,DecimalType(18,0),true    )::
        StructField("RECEIVE_DAY"        ,DecimalType(8,0) ,false)::
        StructField("DESTINATION_ID"     ,DecimalType(10,0),true)::
        Nil
    )

    sql.clear()
    sql.append("select ")
        .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("case when dir=1 then intrunk_carrier_id when dir=2 or dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
        .append("service_type,dir,route_type,switch_id,sum(counts),")
        .append("sum(seconds),ceil(sum(seconds)/60),ceil(sum(seconds)/60),intrunk_carrier_id,")
        .append("outtrunk_carrier_id,route_id,sum(pre_fee),sum (fee),")
        .append(v_month)
        .append(",destination_id ")
        .append("from ")
        .append(groupedCdr)
        .append(" where ")
        .append(v_condition)
        .append(" group by call_year, call_month,call_day, caller_country_id,")
        .append("caller_province_id, caller_zone, caller_carrier_id, caller_type,")
        .append("callee_type, callee_carrier_id, callee_zone, callee_province_id,")
        .append("callee_country_id,")
        .append("case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
        .append("service_type, dir, route_type, switch_id,")
        .append("intrunk_carrier_id, outtrunk_carrier_id, route_id,destination_id")
    val frame = spark.sql(sql.toString())
    val tmpdf = spark.createDataFrame(frame.rdd,schema_day)
    tmpdf.createOrReplaceTempView(CdrDaySumBur)
    SparkUtil.writeDataIntoOrcale(tmpdf,CdrDaySumBur,205)
//----------------------------------------------------------------------------------------------
    val schema_hour = StructType(
      StructField("CALL_YEAR"          ,DecimalType(4,0) ,false)::
      StructField("CALL_MONTH"         ,DecimalType(2,0) ,false)::
      StructField("CALL_DAY"           ,DecimalType(2,0) ,false)::
      StructField("CALL_HOUR"          ,DecimalType(2,0) ,false)::
      StructField("CALLER_COUNTRY_ID"  ,DecimalType(5,0) ,false)::
      StructField("CALLER_PROVINCE_ID" ,DecimalType(5,0) ,false)::
      StructField("CALLER_ZONE"        ,DecimalType(5,0) ,false)::
      StructField("CALLER_CARRIER_ID"  ,DecimalType(5,0) ,false)::
      StructField("CALLER_TYPE"        ,StringType       ,false)::
      StructField("CALLEE_TYPE"        ,StringType       ,false)::
      StructField("CALLEE_CARRIER_ID"  ,DecimalType(5,0) ,false)::
      StructField("CALLEE_ZONE"        ,DecimalType(5,0) ,false)::
      StructField("CALLEE_PROVINCE_ID" ,DecimalType(5,0) ,false)::
      StructField("CALLEE_COUNTRY_ID"  ,DecimalType(5,0) ,false)::
      StructField("TRANS_CO"           ,DecimalType(5,0) ,true    )::
      StructField("SERVICE_TYPE"       ,DecimalType(3,0) ,false)::
      StructField("DIRECTION"          ,DecimalType(2,0) ,false)::
      StructField("ROUTE_TYPE"         ,StringType       ,false)::
      StructField("SWITCH_ID"          ,DecimalType(5,0) ,false)::
      StructField("COUNTS"             ,DecimalType(18,0),true    )::
      StructField("SECONDS"            ,DecimalType(18,0),true    )::
      StructField("BILL_MINUTE"        ,DecimalType(18,0),true    )::
      StructField("SETTLE_MIN"         ,DecimalType(18,0),true    )::
      StructField("INTRUNK_CARRIER_ID" ,DecimalType(5,0) ,false)::
      StructField("OUTTRUNK_CARRIER_ID",DecimalType(5,0) ,false)::
      StructField("ROUTE_ID"           ,DecimalType(10,0),false)::
      StructField("PRE_FEE"            ,DecimalType(18,0),true    )::
      StructField("FEE"                ,DecimalType(18,0),true    )::
      StructField("RECEIVE_DAY"        ,DecimalType(8,0) ,false)::
      StructField("DESTINATION_ID"     ,DecimalType(10,0),true)::
      Nil
     )
    sql.clear()
    sql.append("select ")
      .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("case when dir=1 then intrunk_carrier_id when dir=2 or dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
      .append("service_type,dir,route_type,switch_id,sum(counts),")
      .append("sum(seconds),ceil(sum(seconds)/60),ceil(sum(seconds)/60),intrunk_carrier_id,")
      .append("outtrunk_carrier_id,route_id,sum(pre_fee),sum (fee),")
      .append(v_month)
      .append(",destination_id ")
      .append("from ")
      .append(groupedCdr)
      .append(" where ")
      .append(v_condition)
      .append(" group by call_year, call_month,call_day,call_hour,caller_country_id,")
      .append("caller_province_id, caller_zone, caller_carrier_id, caller_type,")
      .append("callee_type, callee_carrier_id, callee_zone, callee_province_id,")
      .append("callee_country_id,")
      .append("case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
      .append("service_type, dir, route_type, switch_id,")
      .append("intrunk_carrier_id, outtrunk_carrier_id, route_id,destination_id")
    val frame1 = spark.sql(sql.toString())
    val tmpdf1 = spark.createDataFrame(frame1.rdd,schema_day)
    tmpdf1.createOrReplaceTempView(CdrHourSumBur)
    SparkUtil.writeDataIntoOrcale(tmpdf1,CdrHourSumBur,205)
//----------------------------------------------------------------------------------------------
    sql.clear()
    sql.append("select ")
      .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("case when dir=1 then intrunk_carrier_id when dir=2 or dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
      .append("service_type,dir,route_type,switch_id,sum(counts),")
      .append("sum(seconds),ceil(sum(seconds)/60),ceil(sum(seconds)/60),intrunk_carrier_id,")
      .append("outtrunk_carrier_id,route_id,sum(pre_fee),sum (fee),")
      .append(v_month)
      .append(",destination_id ")
      .append("from ")
      .append(CdrDaySumBur)
      .append(" where ")
      .append("service_type != 74 and service_type != 75 ")
    val frame2 = spark.sql(sql.toString())
    val tmpdf2 = spark.createDataFrame(frame2.rdd,schema_day)
    SparkUtil.writeDataIntoOrcale(tmpdf2,DaySumBur,205)

    spark.stop()
  }


  def getCreate_CdrDaySumBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE cdr_day_sum_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }

  def getCreate_DaySumBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE day_sum_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }

  def getCreate_DaySumNobillBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE day_sum_nobill_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }
  def getCreate_CdrHourSumBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE cdr_hour_sum_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALL_HOUR             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }
  def getCreate_HourSumBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE hour_sum_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALL_HOUR            NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }

  def getCreate_HourSumNobillBur_SQL(v_bureau_id:String, p_cycle_id:Int): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE hour_sum_nobill_bur")
      .append(v_bureau_id)
      .append("_")
      .append(p_cycle_id)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALL_HOUR             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(5)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(5)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(5)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(5)  NOT NULL,")
      .append("TRANS_CO             NUMBER(5)  NULL    ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) NULL    ,")
      .append("SECONDS              NUMBER(18) NULL    ,")
      .append("BILL_MINUTE          NUMBER(18) NULL    ,")
      .append("SETTLE_MIN           NUMBER(18) NULL    ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) NULL    ,")
      .append("FEE                  NUMBER(18) NULL    ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) NULL")
      .append(" )")
    sql.toString()
  }
}
