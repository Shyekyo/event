package ideal

import breeze.linalg.sum
import ideal.util.{DBUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by zhangxiaofan on 2019/8/13.
  * /**
  * * +----+----+----+
  * * |key1|key2|key3|
  * * +----+----+----+
  * * | aaa|   1|   2|
  * * | bbb|   3|   4|
  * * | ccc|   3|   5|
  * * | bbb|   4|   6|
  * * +----+----+----+
  * * filter("key2=1")
  * * where("key2=1")
  * * +----+----+----+
  * * |key1|key2|key3|
  * * +----+----+----+
  * * | aaa|   1|   2|
  * * +----+----+----+
  **/
  */
object AddAttribute {
  val _logging = Logger.getLogger(AddAttribute.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  case class DETAIL(caller_num:String,caller_zone:Int,caller_province_id:Int,caller_country_id:Int,caller_type:Int,
                    caller_carrier_id:Int,callee_num:String,callee_zone:Int,callee_province_id:Int,
                    callee_country_id:Int,callee_type:Int,callee_carrier_id:Int,outtrunk_carrier_id:Int,
                    intrunk_carrier_id:Int,out_trunk:String,in_trunk:String,call_year:Int,call_month:Int,
                    call_day:Int,call_hour:Int,call_min:Int,duration:Int,service_type:Int,route_type:Int,
                    switch_id:Int,file_id:String,direction:Int,route_id:Int,pre_fee:Int,fee:Int,calls:Int,
                    destination_id:Int,batchid:Int,third_num:String,rec_type:String,bearer_service:String,
                    trans_param:String,sell_dest_id:String,breau_destination_id:Int,ca_dest_id:Int,settle_month:Int,
                    orig_start_time:String)

  case class DETAIL1(caller_num:String,caller_zone:Int,caller_province_id:Int,caller_country_id:Int,caller_type:Int,
                    caller_carrier_id:Int,callee_num:String,callee_zone:Int,callee_province_id:Int,
                    callee_country_id:Int,callee_type:Int,callee_carrier_id:Int,
                    outtrunk_carrier_id:Int,intrunk_carrier_id:Int,out_trunk:String,
                    in_trunk:String,start_date:String,start_hh:Int,start_miss:Int,duration:Int,
                    service_type:Int,route_type:String,switch_id:Int,file_id:Int,third_num:String,
                    trunk_flag:Int,bearer_service:String,trans_param:String,destination_id:Int,
                    ca_dest_id:Int,sell_dest_id:Int,db_time:Int,s_destination_id:Int,
                    route_id:Int,pre_fee:Int,fee:Int,calls:Int,batchid:Int,rec_type:String,
                    process_time:String,settle_month:Int,orig_start_time:String,trans_co:Int,direction:Int)

  case class DETAIL2(caller_num:String,caller_zone:Int,caller_province_id:Int,caller_country_id:Int,caller_type:Int,
                     caller_carrier_id:Int,callee_num:String,callee_zone:Int,callee_province_id:Int,
                     callee_country_id:Int,callee_type:Int,callee_carrier_id:Int,
                     outtrunk_carrier_id:Int,intrunk_carrier_id:Int,out_trunk:String,
                     in_trunk:String,start_date:String,start_hh:Int,start_miss:Int,duration:Int,
                     service_type:Int,route_type:String,switch_id:Int,file_id:Int,third_num:String,
                     trunk_flag:Int,bearer_service:String,trans_param:String,destination_id:Int,
                     ca_dest_id:Int,sell_dest_id:Int,db_time:Int,s_destination_id:Int,
                     route_id:Int,pre_fee:Int,fee:Int,calls:Int,batchid:Int,rec_type:String,
                     process_time:String,settle_month:Int,orig_start_time:String)
  def main(args: Array[String]): Unit = {
    if(args.length==4){
      val path = args(0)
      val direction = args(1).toInt
      val trans_co_index = args(2).toInt
      val savePath = args(3)
    }
    val path = args(0)
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    calaDaySum(spark,path)
    calaDaySumNobill(spark,path)
    calaHourSum(spark,path)
    calaHourNobillSum(spark,path)
    spark.stop()
  }

  def analyze(spark:SparkSession,path:String,direction:Int,trans_co_index:Int,savePath:String): Unit ={
    import spark.implicits._
    val file = spark.sparkContext.textFile(path)
    //打印日志 计算 duration 总时长
    //val checkCount = file.count()
    //val checkSumDuration = file.map(attr =>attr(21).toInt).sum()
    //_logging.info("{\"originalNum\":"+count+",\"durationSum\":"+sumDuration+"}")
    //checkLog(_logging,checkCount.toString,checkSumDuration.toString)
    val df = file.map(line => line.split(","))
      .map(attr => {
        val trans_co = matchDirection(attr(43).toInt, attr)
        DETAIL(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
        , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
        , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16).toInt, attr(17).toInt
        , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21).toInt, attr(22).toInt, attr(23).toInt
        , attr(24).toInt, attr(25), attr(26).toInt, attr(27).toInt, attr(28).toInt, attr(29).toInt
        , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33), attr(34), attr(35)
        , attr(36), attr(37), attr(38).toInt, attr(39).toInt, attr(40).toInt, attr(41))
  }
      ).toDF()

    df.coalesce(50).write.mode(SaveMode.Append).save(savePath)
  }
  //12 out       13 in
  def matchDirection(direction: Int,attr :Array[String]): String = direction match {
    case 1 => attr(13)
    case 2 => attr(12)
    case 3 => attr(12)
    case 4 => attr(13)
    case _ => "-1"
  }
  def calaDaySum(spark:SparkSession,path:String): Unit ={//788476 925124
    import spark.implicits._
    /*spark.sparkContext.textFile("/detal")
    .map(_.split(","))
    .map(attr =>
      DETAIL(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
        , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
        , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16), attr(17).toInt
        , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21), attr(22).toInt, attr(23).toInt
        , attr(24), attr(25).toInt, attr(26), attr(27), attr(28).toInt, attr(29).toInt
        , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33).toInt, attr(34).toInt, attr(35).toInt
        , attr(36).toInt, attr(37).toInt, attr(38), attr(39), attr(40).toInt, attr(41),attr(42).toInt,attr(43).toInt)
    ).toDS()*/
    val dimStr = daySumDim1(1)
    _logging.info("dimStr => "+dimStr)
    val detail = spark.read.parquet("/detail")
    //val logInfoStr = detail.select("duration").agg("duration" -> "sum", "duration" -> "count").collect().toArray[String]
    //checkLog(_logging,logInfoStr(0),logInfoStr(1))
    //_logging.info("{\"originalNum\":"+logInfoStr.split(",")(0)+",\"durationSum\":"+logInfoStr.split(",")(1)+"}")
    val detailCdrFilterDS = detail.filter("service_type not in(74,75)")
    .where(s"trans_co not in($dimStr,649)")
    .where("(trans_co!=1000 or route_id!=3)")
    detailCdrFilterDS.createOrReplaceTempView("detail_cdr")
    val sql =getDaySumSQL()
    var calaDetailDay = spark.sql(sql)
    calaDetailDay.createOrReplaceTempView("detail_cdr")
    val sb = new StringBuilder()
    sb.append("select call_year,call_month,call_day,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,counts,seconds,bill_minute,settle_min,intrunk_carrier_id,outtrunk_carrier_id,1 as route_id,pre_fee,fee,receive_day,destination_id ")
    sb.append("from detail_cdr")
    val updateRouteId = spark.sql(sb.toString)//925124   8167/8107
    val count = updateRouteId.count()
    SparkUtil.writeDataIntoMysql(updateRouteId,"DAY_SUM_BUR1_201811000")
    _logging.info("{daysumcount :"+count+"}")
  }


  def calaDaySumNobill(spark:SparkSession,path:String): Unit ={//582100  582378
    /*spark.sparkContext.textFile("/detail")
      .map(_.split(","))
      .map(attr =>
        DETAIL(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
          , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
          , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16), attr(17).toInt
          , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21), attr(22).toInt, attr(23).toInt
          , attr(24), attr(25).toInt, attr(26), attr(27), attr(28).toInt, attr(29).toInt
          , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33).toInt, attr(34).toInt, attr(35).toInt
          , attr(36).toInt, attr(37).toInt, attr(38), attr(39), attr(40).toInt, attr(41),attr(42).toInt,attr(43).toInt)
      ).toDS()*/
    val dimStr1 = daySumDim1(1)
    val dimStr2 = daySumDim2(1)
    //createNobillTable("1","b",1811)
    val detailCdr = spark.read.parquet("/detail")
    detailCdr.createOrReplaceTempView("detail_cdr")
    val sql ="select split(start_date,'-')[2] call_year,split(start_date,'-')[1] call_month,split(start_date,'-')[0] call_day,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,sum(calls) counts,sum(duration) seconds,sum(duration)/60 bill_minute,sum(duration)/60 settle_min,intrunk_carrier_id,outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee,substr(settle_month,0,6) receive_day, s_destination_id destination_id from   detail_cdr \ngroup by  split(start_date,'-')[2],split(start_date,'-')[1],split(start_date,'-')[0] ,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,intrunk_carrier_id,outtrunk_carrier_id,route_id,substr(settle_month,0,6),s_destination_id"
    var calaDetailDay = spark.sql(sql)
    val nobillSMS = calaDetailDay.filter("service_type in(74,75)")//短信
      .filter(s"trans_co in (${dimStr2})")
    val nobillInner = calaDetailDay.where(s"trans_co in($dimStr1,649) or (trans_co=1000 and route_id=3)")//中国电信内部
      .filter(s"trans_co in (${dimStr2})")

    SparkUtil.writeDataIntoMysql(nobillSMS,"DAY_SUM_NOBILL_BUR1_201806000")
    SparkUtil.writeDataIntoMysql(nobillInner,"DAY_SUM_NOBILL_BUR1_201806000")
    val nobillSMScount = nobillSMS.count()
    val nobillInnercount = nobillInner.count()
    _logging.info("{daySumNobillSMScount :"+nobillSMScount+"}")
    _logging.info("{daySumNobillInnercount :"+nobillInnercount+"}")
  }
  def createNobillTable(v_bureau_id:String,tail:String,p_cycle_id:Int): Unit ={
    val DaySumNobillBur = s"day_sum_nobill_bur${v_bureau_id}_${p_cycle_id}${tail}"
    if(!DBUtil.tableExists(DaySumNobillBur)){
      //val sql = getCreate_DaySumNobillBur_SQL(v_bureau_id,p_cycle_id)
      //DBUtil.createTable(sql,DaySumNobillBur,destation=205)
    }else{
      DBUtil.clearTable(DaySumNobillBur,205)
    }
  }

  def calaHourSum(spark:SparkSession,path:String): Unit ={ //3300418 3678587
    val dimStr = daySumDim1(1)
    _logging.info("dimStr => "+dimStr)
    val detail = spark.read.parquet("/detail")
    val detailCdrFilterDS = detail.filter("service_type not in(74,75)")
      .where(s"trans_co not in($dimStr,649)")
      .where("(trans_co!=1000 or route_id!=3)")
    detailCdrFilterDS.createOrReplaceTempView("detail_cdr")
    val sql ="select split(start_date,'-')[2] call_year,split(start_date,'-')[1] call_month,split(start_date,'-')[0] call_day,start_hh as call_hour,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,sum(calls) counts,sum(duration) seconds,ceil(sum(duration)/60) bill_minute,ceil(sum(duration)/60) settle_min,intrunk_carrier_id,outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee,substr(settle_month,0,6) receive_day, s_destination_id destination_id from   detail_cdr \ngroup by  split(start_date,'-')[2],split(start_date,'-')[1],split(start_date,'-')[0] ,start_hh,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,intrunk_carrier_id,outtrunk_carrier_id,route_id,substr(settle_month,0,6),s_destination_id"
    var calaDetailHour = spark.sql(sql)
    calaDetailHour.createOrReplaceTempView("detail_cdr")
    val sb = new StringBuilder()
    sb.append("select call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,counts,seconds,bill_minute,settle_min,intrunk_carrier_id,outtrunk_carrier_id,1 as route_id,pre_fee,fee,receive_day,destination_id ")
    sb.append("from detail_cdr")
    val updateRouteId = spark.sql(sb.toString)//925124   8167/8107
    val count = updateRouteId.count()
    SparkUtil.writeDataIntoMysql(updateRouteId,"HOUR_SUM_BUR1_201806000")
    _logging.info("{hourSumCount:"+count+"}")
  }

  def calaHourNobillSum(spark:SparkSession,path:String): Unit ={//2160219 2162387
    val dimStr1 = daySumDim1(1)
    val dimStr2 = daySumDim2(1)
    //createNobillTable("1","b",1811)
    val detailCdr = spark.read.parquet("/detail")
    detailCdr.createOrReplaceTempView("detail_cdr")
    val sql ="select split(start_date,'-')[2] call_year,split(start_date,'-')[1] call_month,split(start_date,'-')[0] call_day,start_hh as call_hour,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,sum(calls) counts,sum(duration) seconds,sum(duration)/60 bill_minute,sum(duration)/60 settle_min,intrunk_carrier_id,outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee,substr(settle_month,0,6) receive_day, s_destination_id destination_id from detail_cdr \ngroup by  split(start_date,'-')[2],split(start_date,'-')[1],split(start_date,'-')[0],start_hh,caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type,direction,route_type,switch_id,intrunk_carrier_id,outtrunk_carrier_id,route_id,substr(settle_month,0,6),s_destination_id"
    var calaDetailHour = spark.sql(sql)
    val nobillSMS = calaDetailHour.filter("service_type in(74,75)")//短信
      .filter(s"trans_co in (${dimStr2})")
    val nobillInner = calaDetailHour.where(s"trans_co in($dimStr1,649) or (trans_co=1000 and route_id=3)")//中国电信内部
      .filter(s"trans_co in (${dimStr2})")
    val nobillSMScount = nobillSMS.count()
    val nobillInnercount = nobillInner.count()
    SparkUtil.writeDataIntoMysql(nobillSMS,"HOUR_SUM_NOBILL_BUR1_201806000")
    SparkUtil.writeDataIntoMysql(nobillInner,"HOUR_SUM_NOBILL_BUR1_201806000")
    _logging.info(" hourSumNobillSMScount => "+nobillSMScount)
    _logging.info(" hourSumNobillInnercount => "+nobillInnercount)
  }

  def daySumDim1(burid:Int): String ={
    val sql="select cust_id from stt_object where substr(cust_code,1,3)='PRO'"+
      " union "+
      s"select cust_id from stt_object where cust_location<>${burid}"
    val cust_id : Array[Array[String]]= DBUtil.queryOra(sql.toString,205)
    val cust_id_str = new StringBuilder()
    for(row <- cust_id){
      cust_id_str.append(row(0)).append(",")
    }
    cust_id_str.substring(0,cust_id_str.length-1).toString
  }

  def daySumDim2(burid:Int): String ={
    val sql=s"select cust_id from stt_object where cust_location=${burid}"
    val cust_id : Array[Array[String]]= DBUtil.queryOra(sql.toString,205)
    val cust_id_str = new StringBuilder()
    for(row <- cust_id){
      cust_id_str.append(row(0)).append(",")
    }
    cust_id_str.substring(0,cust_id_str.length-1).toString
  }

  /**
    *
    * @param spark
    * @param tablename 数据库维表
    */
  def dimTohive(spark:SparkSession,tablename:String,savepath:String,numpartitions:Int=1):Unit={
    val data = spark.read.format("jdbc")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("url", "jdbc:oracle:thin:@//192.9.100.205:1521/ORCL")
      .option("user", "ctgirm").option("password", "Ctgtest_m_205")
      .option("dbtable", tablename).option("fetchsize", 100)
      .load()
    data.repartition(numpartitions).write.save(savepath)
  }

  /**
    *
    * @param _logger 日志
    * @param str 需要cheeck数据的条数
    */
  def checkLog(_logger:Logger,str:String*): Unit ={
    _logger.info("{\"originalNum\":"+str(0)+",\"durationSum\":"+str(1)+"}")
  }

  def getDaySumSQL(): String ={
    val sql ="select split(start_date,'-')[2] call_year,split(start_date,'-')[1] call_month," +
      "split(start_date,'-')[0] call_day,caller_country_id,caller_province_id,caller_zone," +
      "caller_carrier_id,caller_type,callee_type,callee_carrier_id,callee_zone,callee_province_id," +
      "callee_country_id,trans_co,service_type,direction,route_type,switch_id,sum(calls) counts," +
      "sum(duration) seconds,ceil(sum(duration)/60) bill_minute,ceil(sum(duration)/60) settle_min," +
      "intrunk_carrier_id,outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee," +
      "substr(settle_month,0,6) receive_day, s_destination_id destination_id from   detail_cdr " +
      "group by  split(start_date,'-')[2],split(start_date,'-')[1],split(start_date,'-')[0] ," +
      "caller_country_id,caller_province_id,caller_zone,caller_carrier_id,caller_type,callee_type," +
      "callee_carrier_id,callee_zone,callee_province_id,callee_country_id,trans_co,service_type," +
      "direction,route_type,switch_id,intrunk_carrier_id,outtrunk_carrier_id,route_id," +
      "substr(settle_month,0,6),s_destination_id"
    sql
  }
}
