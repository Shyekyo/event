package ideal

import ideal.hive_fix_month_sum._logging
import ideal.util.{DBUtil, DateUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhangxiaofan on 2019/7/30.
  */
object part_fix_month_day_sum {
  val _logging = Logger.getLogger(Int_fix_month_sum.getClass)
  var p_acct_data_class=31
  var p_cycle_id = 201806000
  var p_bureau_id = 1//1大陆 3香港 4欧洲

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    if (args.length == 3) {
      p_acct_data_class = args(0).toInt
      p_cycle_id = args(1).toInt
      p_bureau_id = args(2).toInt
    }
    _logging.info("p_acct_data_class => " + p_acct_data_class + " ,p_cycle_id => " + p_cycle_id + " ,p_bureau_id => " + p_bureau_id)
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    var v_condition: String = null
    var v_first_date: String = null
    var v_last_date: String = null

    val v_acct_data_class = p_acct_data_class.toString
    val v_bureau_id = p_bureau_id.toString
    val v_file_type = p_bureau_id.toString
    val v_bill_id = p_cycle_id
    val v_bill_id_char = p_cycle_id.toString
    var v_month = v_bill_id_char.substring(0, 6)
    val v_start_time = DateUtil.getToday()
    v_condition = s" settle_month =  ${p_cycle_id}"
    _logging.info("v_condition : " + v_condition)

    val df = analyze(spark,"a","1")
    SparkUtil.writeDataIntoCTG(df,"day_sum_bur1_201806_TMP")
    spark.stop()
  }

  def analyze(spark:SparkSession,dsName:String,bureau_id:String): DataFrame ={
    val cust_id_pro = getConditionPro()
    _logging.info("CUST_ID_PRO => "+cust_id_pro)
    val cust_id_location = getConditionLocation(bureau_id)
    _logging.info("CUST_ID_LOCATION => "+cust_id_location)
    val sql = new StringBuilder()
    sql.append("select ")
      .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end as trans_co,service_type,")
      .append("dir as direction,route_type,switch_id,sum(counts) as counts,sum(seconds) as seconds,")
      .append("ceil(sum(seconds)/60) as bill_minute,ceil(sum(seconds)/60) as settle_min,intrunk_carrier_id,outtrunk_carrier_id,1 as route_id,")
      .append("sum(pre_fee) as pre_fee,sum(fee) as fee,201806 as receive_day,destination_id ")
      .append(" from ")
      .append("grouped_cdr_201806_csv")
      .append(" where ")
      .append("settle_month = 201806000")
      .append(" and service_type != 74 and service_type != 75")
      .append(" and case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end not in")
      .append(s"($cust_id_pro)")
      .append(" and case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end not in")
      .append(s"($cust_id_location)")
      .append(" and case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end!=649")
      .append(" and (route_id!=3 or case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end!=1000)")
      .append(" group by ")
      .append(" call_year, call_month,call_day,caller_country_id,")
      .append(" caller_province_id, caller_zone, caller_carrier_id, caller_type,")
      .append(" callee_type, callee_carrier_id, callee_zone, callee_province_id,callee_country_id,")
      .append(" case when dir=1 then intrunk_carrier_id when dir=2 OR dir=3 then outtrunk_carrier_id else intrunk_carrier_id end,")
      .append(" service_type, dir,route_type,switch_id,intrunk_carrier_id,outtrunk_carrier_id,destination_id")
    spark.sql(sql.toString)
  }


  def getConditionPro():String={
    val sql:StringBuilder=new StringBuilder()
    sql.append("select cust_id from stt_object where substr(cust_code,1,3)='PRO'")
    val cust_id : Array[Array[String]]= DBUtil.queryOra(sql.toString,205)
    val cust_id_str = new StringBuilder()
    for(row <- cust_id){
      cust_id_str.append(row(0)).append(",")
    }
    cust_id_str.substring(0,cust_id_str.length-1).toString
  }
  def getConditionLocation(bureau_id:String):String={
    val sql:StringBuilder=new StringBuilder()
    sql.append(s"select cust_id from stt_object where cust_location!=${bureau_id}")
    val cust_id_location= DBUtil.queryOra(sql.toString,205)
    val cust_id_location_str = new StringBuilder()
    for(row <- cust_id_location){
      cust_id_location_str.append(row(0)).append(",")
    }
    cust_id_location_str.substring(0,cust_id_location_str.length-1).toString
  }

}
