package ideal

import java.util.Properties

import ideal.constants.Constants
import ideal.util.SparkUtil.getOracleCtgirmProps
import ideal.util.{DBUtil, DateUtil, SparkUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

/**
  * Created by zhangxiaofan on 2019/7/24.
  */
object hive_fix_month_sum {
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
    _logging.info("p_acct_data_class => "+p_acct_data_class+" ,p_cycle_id => "+p_cycle_id+" ,p_bureau_id => "+p_bureau_id )
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //config("spark.sql.warehouse.dir", prefix2+"spark-warehouse").
      .getOrCreate()
    import spark.implicits._







//====================================
    var v_condition : String = null
    var v_first_date : String = null
    var v_last_date : String = null

    _logging.info("1 step...................")
    val v_acct_data_class = p_acct_data_class.toString
    val v_bureau_id =p_bureau_id.toString
    val v_file_type =p_bureau_id.toString
    val v_bill_id = p_cycle_id
    val v_bill_id_char = p_cycle_id.toString
    var v_month = v_bill_id_char.substring(0,6)
    val v_start_time = DateUtil.getToday()

    /*if  (p_bureau_id==1 || p_bureau_id==3){
      v_condition = s" route_id in (1,3) and settle_month = ${p_cycle_id}"
    }else{
      v_condition = s" route_id=${p_bureau_id} and settle_month = ${p_cycle_id}"
    }*/
    v_condition = s" settle_month =  ${p_cycle_id}"
    _logging.info("v_condition : "+v_condition)

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
    // 第14 步 用 到
    //select cust_id from stt_object where substr(cust_code,1,3)=''PRO''
    sql.clear()
    sql.append("select cust_id from stt_object where substr(cust_code,1,3)='PRO'")
    val cust_id : Array[Array[String]]= DBUtil.queryOra(sql.toString,205)
    val cust_id_str = new StringBuilder()
    for(row <- cust_id){
      cust_id_str.append(row(0)).append(",")
    }
    val cust_id_str_result = cust_id_str.substring(0,cust_id_str.length-1)
    _logging.info("CUST_ID_STR_RESULT : "+cust_id_str_result)
    //select cust_id from stt_object where cust_location<>'||v_bureau_id ||'
    sql.clear()
    sql.append(s"select cust_id from stt_object where cust_location<>${v_bureau_id}")
    val cust_id_no_bureau_id= DBUtil.queryOra(sql.toString,205)
    val cust_id_no_bureau_id_str = new StringBuilder()
    for(row <- cust_id_no_bureau_id){
      cust_id_no_bureau_id_str.append(row(0)).append(",")
    }
    val cust_id_no_bureau_id_str_result = cust_id_no_bureau_id_str.substring(0,cust_id_no_bureau_id_str.length-1)
    _logging.info("CUST_ID_NO_BUREAU_ID_STR_RESULT : "+cust_id_no_bureau_id_str_result)

    //select cust_id from stt_object where cust_location=1
    sql.clear()
    sql.append(s"select cust_id from stt_object where cust_location=1")
    val cust_id_location1= DBUtil.queryOra(sql.toString,205)
    val cust_id_location1_str = new StringBuilder()
    for(row <- cust_id_location1){
      cust_id_location1_str.append(row(0)).append(",")
    }
    val cust_id_location_str_result1 = cust_id_location1_str.substring(0,cust_id_location1_str.length-1)
    _logging.info("CUST_ID_LOCATION_STR_RESULT1 : "+cust_id_location_str_result1)

    //select cust_id from stt_object where cust_location=3
    sql.clear()
    sql.append(s"select cust_id from stt_object where cust_location=3")
    val cust_id_location3= DBUtil.queryOra(sql.toString,205)
    val cust_id_location3_str = new StringBuilder()
    for(row <- cust_id_location3){
      cust_id_location3_str.append(row(0)).append(",")
    }
    val cust_id_location_str_result3 = cust_id_location3_str.substring(0,cust_id_location3_str.length-1)
    _logging.info("CUST_ID_LOCATION_STR_RESULT3 : "+cust_id_location_str_result3)

    //select cust_id from stt_object where cust_location=
    sql.clear()
    sql.append(s"select cust_id from stt_object where cust_location=${v_bureau_id}")
    val cust_id_location4= DBUtil.queryOra(sql.toString,205)
    val cust_id_location4_str = new StringBuilder()
    for(row <- cust_id_location4){
      cust_id_location4_str.append(row(0)).append(",")
    }
    val cust_id_location_str_result4 = cust_id_location4_str.substring(0,cust_id_location4_str.length-1)
    _logging.info("CUST_ID_LOCATION_STR_RESULT4 : "+cust_id_location_str_result4)


    //DELETE FROM settle_carrier WHERE BILLING_CYCLE_ID = ${p_cycle_id} and bureau_id = ${p_bureau_id};
    sql.clear()
    val settle_carrier_spark ="settle_carrier_spark"
    sql.append(s"DELETE FROM ${settle_carrier_spark} WHERE BILLING_CYCLE_ID = ${p_cycle_id} and bureau_id = ${p_bureau_id}")
    _logging.info(sql)
    DBUtil.deleteOra(sql.toString,205)


    //====
    _logging.info("第二部：确认cdr_day_sum_bur[bureau_id]_[cycle_id]是否存在")

    val tail = "T"
    val CdrDaySumBur = s"cdr_day_sum_bur${v_bureau_id}_${p_cycle_id}${tail}".toUpperCase
    _logging.info(CdrDaySumBur)
    if(!DBUtil.tableExists(CdrDaySumBur,205)){
      val sqlCdrDaySumBur = getCreate_CdrDaySumBur_SQL(CdrDaySumBur)
      DBUtil.createTable(sqlCdrDaySumBur,CdrDaySumBur,destation=205)
    }else{
      DBUtil.clearTable(CdrDaySumBur,205)
    }
    _logging.info("第三部：确认cdr_hour_sum_bur[bureau_id]_[cycle_id]是否存在")


    val CdrHourSumBur = s"cdr_hour_sum_bur${v_bureau_id}_${p_cycle_id}${tail}".toUpperCase
    _logging.info(CdrHourSumBur)
    if(!DBUtil.tableExists(CdrHourSumBur,205)){
      val sqlCdrHourSumBur = getCreate_CdrHourSumBur_SQL(CdrHourSumBur)
      DBUtil.createTable(sqlCdrHourSumBur,CdrHourSumBur,destation=205)
    }else{
      DBUtil.clearTable(CdrHourSumBur,205)
    }
    _logging.info("第四部：确认day_sum_bur[bureau_id]_[cycle_id]是否存在")

    val DaySumBur = s"day_sum_bur${v_bureau_id}_${p_cycle_id}${tail}".toUpperCase
    _logging.info(DaySumBur)
    if(!DBUtil.tableExists(DaySumBur,205)){
      val sqlDaySumBur = getCreate_DaySumBur_SQL(DaySumBur)
      DBUtil.createTable(sqlDaySumBur,DaySumBur,destation=205)
    }else{
      DBUtil.clearTable(DaySumBur,205)
    }
    _logging.info("第五部：确认day_sum_nobill_bur[bureau_id]_[cycle_id]是否存在")

    val DaySumNobillBur = s"day_sum_nobill_bur${v_bureau_id}_$p_cycle_id${tail}".toUpperCase
    _logging.info(DaySumNobillBur)
    if(!DBUtil.tableExists(DaySumNobillBur,205)){
      val sqlDaySumNobillBur = getCreate_DaySumNobillBur_SQL(DaySumNobillBur)
      DBUtil.createTable(sqlDaySumNobillBur,DaySumNobillBur,destation=205)
    }else{
      DBUtil.clearTable(DaySumNobillBur,205)
    }
    _logging.info("第七部：确认hour_sum_bur[bureau_id]_[cycle_id]是否存在")

    val HourSumBur = s"hour_sum_bur${v_bureau_id}_$p_cycle_id${tail}".toUpperCase
    _logging.info(HourSumBur)
    if(!DBUtil.tableExists(HourSumBur,205)){
      val sqlHourSumBur = getCreate_HourSumBur_SQL(HourSumBur)
      DBUtil.createTable(sqlHourSumBur,HourSumBur,destation=205)
    }else{
      DBUtil.clearTable(HourSumBur,205)
    }
    _logging.info("第八部：确认hour_sum_nobill_bur[bureau_id]_[cycle_id]是否存在")

    val HourSumNobillBur = s"hourSumNobillBur${v_bureau_id}_$p_cycle_id${tail}".toUpperCase
    _logging.info(HourSumNobillBur)
    if(!DBUtil.tableExists(HourSumNobillBur,205)){
      val sqlHourSumNobillBur = getCreate_HourSumNobillBur_SQL(HourSumNobillBur)
      DBUtil.createTable(sqlHourSumNobillBur,HourSumNobillBur,destation=205)
    }else{
      DBUtil.clearTable(HourSumNobillBur,205)
    }
    _logging.info("第十部：汇总group_cdr_[month]表数据入cdr_day_sum_bur[bureau_id]_[cycle_id]")
    val groupedCdr = s"grouped_cdr_${v_month}_csv"
    sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("case when dir=1 then intrunk_carrier_id when dir=2 or dir=3 then outtrunk_carrier_id else intrunk_carrier_id end as trans_co,")
        .append("service_type,dir as direction,route_type,switch_id,sum(counts) as counts,")
        .append("sum(seconds) as seconds,ceil(sum(seconds)/60) as bill_minute,ceil(sum(seconds)/60) as settle_min,intrunk_carrier_id,")
        .append("outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee,")
        .append(v_month).append(" as receive_day")
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
    _logging.info(sql.toString()+".....")
    val frame = spark.sql(sql.toString())
    //frame.show()
    frame.createOrReplaceTempView(CdrDaySumBur)
    SparkUtil.writeDataIntoCTG(frame,CdrDaySumBur)
    //----------------------------------------------------------------------------------------------
    _logging.info("第十一部：汇总group_cdr_[month]表数据入cdr_hour_sum_bur[bureau_id]_[cycle_id]")
      sql.clear()
          sql.append("select ")
            .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
            .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
            .append("callee_zone,callee_province_id, callee_country_id,")
            .append("case when dir=1 then intrunk_carrier_id when dir=2 or dir=3 then outtrunk_carrier_id else intrunk_carrier_id end as trans_co,")
            .append("service_type,dir as direction,route_type,switch_id,sum(counts) as counts,")
            .append("sum(seconds) as seconds,ceil(sum(seconds)/60) as bill_minute,ceil(sum(seconds)/60) as settle_min,intrunk_carrier_id,")
            .append("outtrunk_carrier_id,route_id,sum(pre_fee) as pre_fee,sum(fee) as fee,")
            .append(v_month).append(" as receive_day")
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
          _logging.info(sql.toString()+".....")
      val frame1 = spark.sql(sql.toString())
      //val tmpdf1 = spark.createDataFrame(frame1.rdd,schema_day)
      //frame1.show()
      frame1.createOrReplaceTempView(CdrHourSumBur)
      //SparkUtil.writeDataIntoOrcale(tmpdf1,CdrHourSumBur,205)
      SparkUtil.writeDataIntoCTG(frame1,CdrHourSumBur)

    _logging.info("第十二部：cdr_day_sum_bur[bureau_id]_[cycle_id]表数据去除掉业务类型为\"全球虚拟总机\"和“**133回拨平台”的数据，新增入day_sum_bur[bureau_id]_[cycle_id]")
    sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("trans_co,")
        .append("service_type,direction,route_type,switch_id,counts,")
        .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
        .append("outtrunk_carrier_id,route_id,pre_fee,fee,")
        .append(v_month).append(" as receive_day")
        .append(",destination_id ")
        .append("from ")
        .append(CdrDaySumBur)
        .append(" where ")
        .append("service_type != 74 and service_type != 75 ")
    _logging.info(sql.toString()+".....")
    val frame2 = spark.sql(sql.toString())
    //frame2.show()
    frame2.createOrReplaceTempView(DaySumBur)
    SparkUtil.writeDataIntoCTG(frame2,DaySumBur)
    //----------------------------------------------------------------------------------------------
    _logging.info("第十三部：cdr_day_sum_bur[bureau_id]_[cycle_id]表数据去除掉业务类型为\"全球虚拟总机\"和“**133回拨平台”的数据，新增入day_sum_bur[bureau_id]_[cycle_id]")
    sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,trans_co,")
        .append("service_type,direction,route_type,switch_id,counts,")
        .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
        .append("outtrunk_carrier_id,route_id,pre_fee,fee,receive_day")
        .append(",destination_id ")
          .append(" from ")
          .append(CdrDaySumBur)
          .append(" where service_type in (74,75)")
    _logging.info(sql.toString()+".....")
    val frame3 = spark.sql(sql.toString())
    //frame3.show()
    SparkUtil.writeDataIntoCTG(frame3,DaySumNobillBur)
    //----------------------------------------------------------------------------------------------
    _logging.info("第十四部：删除中国电信内部运营商数据")
    sql.clear()
    sql.append("select * from ").append(DaySumBur).append(" where trans_co in (")
       .append(cust_id_str_result).append(")")
    _logging.info(sql.toString()+"!!!!")
    val frame4 = spark.sql(sql.toString())
    frame4.schema
    //frame4.show()
    SparkUtil.writeDataIntoCTG(frame4,DaySumNobillBur)
    sql.clear()
    //sql.append("delete from ").append(DaySumBur).append(" where trans_co  in (")
    //  .append(cust_id_str_result).append(")")
      sql.append("select * from ").append(DaySumBur).append(" where trans_co not in (")
        .append(cust_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val daySumBurNotIN1 = spark.sql(sql.toString())
    //spark.catalog.dropTempView(DaySumBur)
    daySumBurNotIN1.createOrReplaceTempView(DaySumBur)
    //----------------------------------------------------------------------------------------------
    _logging.info("第十五部：只结算当前账务主体的运营商..")
    sql.clear()
      sql.append("select * from ").append(DaySumBur).append(" where trans_co in (")
        .append(cust_id_no_bureau_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val frame5 = spark.sql(sql.toString())
    //frame5.show()
    SparkUtil.writeDataIntoCTG(frame5,DaySumNobillBur)
    sql.clear()
      sql.append("select * from ").append(DaySumBur).append(" where trans_co not in (")
        .append(cust_id_no_bureau_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val daySumBurNotIN2 = spark.sql(sql.toString())
    daySumBurNotIN2.createOrReplaceTempView(DaySumBur)

    if(p_bureau_id==1){
      _logging.info("CT大陆数据")
      sql.clear()
        sql.append("select * from ").append(DaySumBur).append(" where trans_co=649")
      _logging.info(sql.toString()+".....")
      val frame6 = spark.sql(sql.toString())
      //frame6.show()
      SparkUtil.writeDataIntoCTG(frame6,DaySumNobillBur)
      _logging.info("--删除和国际公司结算的数据")
      sql.clear()
        sql.append("select * from ").append(DaySumBur).append(" where trans_co!=649")
      _logging.info(sql.toString()+".....")
      val daySumBurNeq = spark.sql(sql.toString())
      daySumBurNeq.createOrReplaceTempView(DaySumBur)
      sql.clear()
        sql.append("select * from ").append(DaySumBur).append(" where route_id=3 and trans_co=1000")
      _logging.info(sql.toString()+".....")
      val frame7 = spark.sql(sql.toString())
      //frame7.show()
      SparkUtil.writeDataIntoCTG(frame7,DaySumNobillBur)
      _logging.info("-- 删除交换机产生的和大陆结算的数据")
      sql.clear()
        sql.append("select * from ").append(DaySumBur).append(" where route_id!=3 and trans_co!=1000")
      _logging.info(sql.toString()+".....")
      val daySumBurNeq2 = spark.sql(sql.toString())
      daySumBurNeq2.createOrReplaceTempView(DaySumBur)
      sql.clear()
        sql.append("select ")
          .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
          .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
          .append("callee_zone,callee_province_id, callee_country_id,")
          .append("trans_co,")
          .append("service_type,direction,route_type,switch_id,counts,")
          .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
          .append("outtrunk_carrier_id,1,pre_fee,fee,")
          .append(v_month).append(" as receive_day")
          .append(",destination_id ")
          .append("from ")
          .append(DaySumBur)
      //sql.append("update ").append(DaySumBur).append(" set route_id=1")
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      val daySumBurRout1 = spark.sql(sql.toString())
      daySumBurRout1.createOrReplaceTempView(DaySumBur)
      //delete from day_sum_nobill_bur'||v_bureau_id||'_'|| p_cycle_id
      //      || ' where trans_co not in ( select cust_id from stt_object where cust_location=1 )';
      sql.clear()
      sql.append("delete from ").append(DaySumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result1).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Day_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)
      _logging.info("day p_bureau_id==1 .... end")
    }
    else if(p_bureau_id==3){
      _logging.info("--CTHK  香港数据")
      sql.clear()
      sql.append("select * from ").append(DaySumBur).append(" where route_id=3 and trans_co=1000")
      _logging.info(sql.toString()+".....")
      val frame7 = spark.sql(sql.toString())
      //frame7.show()
      SparkUtil.writeDataIntoCTG(frame7,DaySumNobillBur)
      _logging.info("-- 删除交换机产生的和大陆结算的数据")
      sql.clear()
      sql.append("select * from ").append(DaySumBur).append(" where route_id!=3 and trans_co!=1000")
      _logging.info(sql.toString()+".....")
      val daySumBurNeq2 = spark.sql(sql.toString())
      daySumBurNeq2.createOrReplaceTempView(DaySumBur)
      sql.clear()
        sql.append("select ")
          .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
          .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
          .append("callee_zone,callee_province_id, callee_country_id,")
          .append("trans_co,")
          .append("service_type,direction,route_type,switch_id,counts,")
          .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
          .append("outtrunk_carrier_id,3,pre_fee,fee,")
          .append(v_month).append(" as receive_day")
          .append(",destination_id ")
          .append("from ")
          .append(DaySumBur)
      //sql.append("update ").append(DaySumBur).append(" set route_id=1")
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      val daySumBurRout3 = spark.sql(sql.toString())
      daySumBurRout3.createOrReplaceTempView(DaySumBur)
      sql.clear()
      sql.append("delete from ").append(DaySumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result3).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Day_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)

    }else{
      _logging.info("-- modify by qiulb")
      sql.clear()
      //sql.append("update ").append(DaySumBur).append(s" set route_id=${v_bureau_id}")
        sql.append("select ")
          .append("call_year,call_month,call_day,caller_country_id,caller_province_id,")
          .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
          .append("callee_zone,callee_province_id, callee_country_id,")
          .append("trans_co,")
          .append("service_type,direction,route_type,switch_id,counts,")
          .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
          .append(s"outtrunk_carrier_id,${v_bureau_id},pre_fee,fee,")
          .append(v_month).append(" as receive_day")
          .append(",destination_id ")
          .append("from ")
          .append(DaySumBur)
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      sql.clear()
      sql.append("delete from ").append(DaySumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result4).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Day_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)
    }
//201806 as settle_month,trans_co as carrier_id,4 as bureau_ia,201806000 as billing_cycle_id
    /*sql.clear()
    sql.append(s"select ${v_month} as settle_month, trans_co as carrier_id,${v_bureau_id} as bureau_ia,${p_cycle_id}  as billing_cycle_id from ")
      .append(DaySumBur).
          append(s" group by  ${v_month}, trans_co,${v_bureau_id} ,${p_cycle_id}")
    _logging.info(sql.toString()+".....")
    val frame8 = spark.sql(sql.toString())
    SparkUtil.writeDataIntoCTG(frame8,s"${settle_carrier_spark}")*/

//=========================================================================================================
    _logging.info("-- 小时汇总数据处理")
    sql.clear()
    sql.append("select ")
      .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("trans_co,")
      .append("service_type,direction,route_type,switch_id,counts,")
      .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
      .append("outtrunk_carrier_id,route_id,pre_fee,fee,")
      .append(v_month).append(" as receive_day")
      .append(",destination_id ")
      .append("from ")
      .append(CdrHourSumBur)
      .append(" where ")
      .append("service_type != 74 and service_type != 75 ")
    _logging.info(sql.toString()+".....")
    val frame9 = spark.sql(sql.toString())
    //frame2.show()
    frame9.createOrReplaceTempView(HourSumBur)
    _logging.info("--删除中国电信内部运营商结算数据")
    sql.clear()
    sql.append("select * from ").append(HourSumBur).append(" where trans_co in (")
      .append(cust_id_str_result).append(")")
    _logging.info(sql.toString()+"!!!!")
    val frame10 = spark.sql(sql.toString())
    frame10.schema
    //frame4.show()
    SparkUtil.writeDataIntoCTG(frame10,HourSumNobillBur)
    sql.clear()
    sql.append("select * from ").append(HourSumBur).append(" where trans_co not in (")
      .append(cust_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val hourSumBurNotIN1 = spark.sql(sql.toString())
    //spark.catalog.dropTempView(DaySumBur)
    hourSumBurNotIN1.createOrReplaceTempView(HourSumBur)

    sql.clear()
    sql.append("select * from ").append(HourSumBur).append(" where trans_co in (")
      .append(cust_id_no_bureau_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val frame11 = spark.sql(sql.toString())
    //frame5.show()
    SparkUtil.writeDataIntoCTG(frame11,HourSumNobillBur)
    sql.clear()
    sql.append("select * from ").append(HourSumBur).append(" where trans_co not in (")
      .append(cust_id_no_bureau_id_str_result).append(")")
    _logging.info(sql.toString()+".....")
    val hourSumBurNotIN2 = spark.sql(sql.toString())
    hourSumBurNotIN2.createOrReplaceTempView(HourSumBur)



    if(p_bureau_id==1){
      _logging.info(" hour CT大陆数据")
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where trans_co=649")
      _logging.info(sql.toString()+".....")
      val frame12 = spark.sql(sql.toString())
      SparkUtil.writeDataIntoCTG(frame12,HourSumNobillBur)
      _logging.info(" hour --删除和国际公司结算的数据")
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where trans_co!=649")
      _logging.info(sql.toString()+".....")
      val hourSumBurNeq = spark.sql(sql.toString())
      hourSumBurNeq.createOrReplaceTempView(HourSumBur)
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where route_id=3 and trans_co=1000")
      _logging.info(sql.toString()+".....")
      val frame13 = spark.sql(sql.toString())
      SparkUtil.writeDataIntoCTG(frame13,HourSumNobillBur)
      _logging.info(" hour -- 删除交换机产生的和大陆结算的数据")
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where route_id!=3 and trans_co!=1000")
      _logging.info(sql.toString()+".....")
      val hourSumBurNeq2 = spark.sql(sql.toString())
      hourSumBurNeq2.createOrReplaceTempView(HourSumBur)
      sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("trans_co,")
        .append("service_type,direction,route_type,switch_id,counts,")
        .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
        .append("outtrunk_carrier_id,1 as route_id,pre_fee,fee,")
        .append(v_month).append(" as receive_day")
        .append(",destination_id ")
        .append("from ")
        .append(HourSumBur)
      //sql.append("update ").append(DaySumBur).append(" set route_id=1")
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      val hourSumBurRout1 = spark.sql(sql.toString())
      hourSumBurRout1.createOrReplaceTempView(HourSumBur)
      //delete from day_sum_nobill_bur'||v_bureau_id||'_'|| p_cycle_id
      //      || ' where trans_co not in ( select cust_id from stt_object where cust_location=1 )';
      sql.clear()
      sql.append("delete from ").append(HourSumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result1).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Hour_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)
      _logging.info("hour p_bureau_id==1 .... end")
    }
    else if(p_bureau_id==3){
      _logging.info(" hour --CTHK  香港数据")
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where route_id=3 and trans_co=1000")
      _logging.info(sql.toString()+".....")
      val frame13 = spark.sql(sql.toString())
      SparkUtil.writeDataIntoCTG(frame13,HourSumNobillBur)
      _logging.info(" hour -- 删除交换机产生的和大陆结算的数据")
      sql.clear()
      sql.append("select * from ").append(HourSumBur).append(" where route_id!=3 and trans_co!=1000")
      _logging.info(sql.toString()+".....")
      val hourSumBurNeq2 = spark.sql(sql.toString())
      hourSumBurNeq2.createOrReplaceTempView(HourSumBur)
      sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("trans_co,")
        .append("service_type,direction,route_type,switch_id,counts,")
        .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
        .append("outtrunk_carrier_id,3 as route_id,pre_fee,fee,")
        .append(v_month).append(" as receive_day")
        .append(",destination_id ")
        .append("from ")
        .append(HourSumBur)
      //sql.append("update ").append(DaySumBur).append(" set route_id=1")
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      val hourSumBurRout3 = spark.sql(sql.toString())
      hourSumBurRout3.createOrReplaceTempView(HourSumBur)
      sql.clear()
      sql.append("delete from ").append(HourSumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result3).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Hour_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)
      _logging.info("hour p_bureau_id==3 .... end")
    }else{
      _logging.info(" hour -- modify by qiulb")
      sql.clear()
      sql.append("select ")
        .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
        .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
        .append("callee_zone,callee_province_id, callee_country_id,")
        .append("trans_co,")
        .append("service_type,direction,route_type,switch_id,counts,")
        .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
        .append(s"outtrunk_carrier_id,${v_bureau_id} as route_id,pre_fee,fee,")
        .append(v_month).append(" as receive_day")
        .append(",destination_id ")
        .append("from ")
        .append(DaySumBur)
      _logging.info(sql.toString()+".....")
      spark.sql(sql.toString())
      sql.clear()
      sql.append("delete from ").append(HourSumNobillBur).append(" where trans_co not in (")
        .append(cust_id_location_str_result4).append(")")
      _logging.info(sql.toString()+".....")
      //spark.sql(sql.toString())
      val del_Hour_Sum_Nobill_Bur= DBUtil.deleteOra(sql.toString,205)
      _logging.info("hour p_bureau_id==4 .... end")
    }

    _logging.info("hour--删除业务类型=74、75的数据")
    sql.clear()
    sql.append("select ")
      .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("trans_co,")
      .append("service_type,direction,route_type,switch_id,counts,")
      .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
      .append("outtrunk_carrier_id,route_id,pre_fee,fee,")
      .append(v_month).append(" as receive_day")
      .append(",destination_id ")
      .append("from ")
      .append(HourSumBur)
      .append(" where ")
      .append("service_type in (74,75)")
    _logging.info(sql.toString()+".....")
    val frame14 = spark.sql(sql.toString())
    frame14.createOrReplaceTempView(HourSumBur)
    SparkUtil.writeDataIntoCTG(frame14,HourSumNobillBur)
    _logging.info("hour--trans_co在stt_object_int_comp表里的，放在香港公司结算")
    sql.clear()
    sql.append("select ")
      .append("call_year,call_month,call_day,call_hour,caller_country_id,caller_province_id,")
      .append("caller_zone,caller_carrier_id,caller_type,callee_type,callee_carrier_id,")
      .append("callee_zone,callee_province_id, callee_country_id,")
      .append("trans_co,")
      .append("service_type,direction,route_type,switch_id,counts,")
      .append("seconds,bill_minute,settle_min,intrunk_carrier_id,")
      .append("outtrunk_carrier_id,route_id,pre_fee,fee,")
      .append(v_month).append(" as receive_day")
      .append(",destination_id ")
      .append("from ")
      .append(HourSumBur)
      .append(" where ")
      .append("service_type not in (74,75)")
    _logging.info(sql.toString()+".....")
    val frame15 = spark.sql(sql.toString())
    SparkUtil.writeDataIntoCTG(frame15,HourSumBur)
    spark.stop()
  }

  def getCreate_CdrDaySumBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  ,")
      .append("CALL_MONTH           NUMBER(2)  ,")
      .append("CALL_DAY             NUMBER(2)  ,")
      .append("CALLER_COUNTRY_ID    NUMBER(6)  ,")
      .append("CALLER_PROVINCE_ID   NUMBER(6)  ,")
      .append("CALLER_ZONE          NUMBER(6)  ,")
      .append("CALLER_CARRIER_ID    NUMBER(6)  ,")
      .append("CALLER_TYPE          CHAR(1)    ,")
      .append("CALLEE_TYPE          CHAR(1)    ,")
      .append("CALLEE_CARRIER_ID    NUMBER(6)  ,")
      .append("CALLEE_ZONE          NUMBER(6)  ,")
      .append("CALLEE_PROVINCE_ID   NUMBER(6)  ,")
      .append("CALLEE_COUNTRY_ID    NUMBER(6)  ,")
      .append("TRANS_CO             NUMBER(6)  ,")
      .append("SERVICE_TYPE         NUMBER(6)  ,")
      .append("DIRECTION            NUMBER(6)  ,")
      .append("ROUTE_TYPE           CHAR(1)    ,")
      .append("SWITCH_ID            NUMBER(6)  ,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(6)  ,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(6)  ,")
      .append("ROUTE_ID             NUMBER(10) ,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  ,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }

  def getCreate_DaySumBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(6)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(6)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(6)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(6)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(6)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(6)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(6)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(6)  NOT NULL,")
      .append("TRANS_CO             NUMBER(6)  ,")
      .append("SERVICE_TYPE         NUMBER(6)  NOT NULL,")
      .append("DIRECTION            NUMBER(6)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(6)  NOT NULL,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(6)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(6)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }

  def getCreate_DaySumNobillBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
      .append(" (")
      .append("CALL_YEAR            NUMBER(4)  NOT NULL,")
      .append("CALL_MONTH           NUMBER(2)  NOT NULL,")
      .append("CALL_DAY             NUMBER(2)  NOT NULL,")
      .append("CALLER_COUNTRY_ID    NUMBER(6)  NOT NULL,")
      .append("CALLER_PROVINCE_ID   NUMBER(6)  NOT NULL,")
      .append("CALLER_ZONE          NUMBER(6)  NOT NULL,")
      .append("CALLER_CARRIER_ID    NUMBER(6)  NOT NULL,")
      .append("CALLER_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_TYPE          CHAR(1)    NOT NULL,")
      .append("CALLEE_CARRIER_ID    NUMBER(6)  NOT NULL,")
      .append("CALLEE_ZONE          NUMBER(6)  NOT NULL,")
      .append("CALLEE_PROVINCE_ID   NUMBER(6)  NOT NULL,")
      .append("CALLEE_COUNTRY_ID    NUMBER(6)  NOT NULL,")
      .append("TRANS_CO             NUMBER(6)  ,")
      .append("SERVICE_TYPE         NUMBER(6)  NOT NULL,")
      .append("DIRECTION            NUMBER(6)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(6)  NOT NULL,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(6)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(6)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }
  def getCreate_CdrHourSumBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
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
      .append("TRANS_CO             NUMBER(5)  ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }
  def getCreate_HourSumBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
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
      .append("TRANS_CO             NUMBER(5)  ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }

  def getCreate_HourSumNobillBur_SQL(tableName:String): String ={
    val sql = new StringBuilder()
    sql.append("CREATE TABLE ")
      .append(tableName)
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
      .append("TRANS_CO             NUMBER(5)  ,")
      .append("SERVICE_TYPE         NUMBER(3)  NOT NULL,")
      .append("DIRECTION            NUMBER(2)  NOT NULL,")
      .append("ROUTE_TYPE           CHAR(1)    NOT NULL,")
      .append("SWITCH_ID            NUMBER(5)  NOT NULL,")
      .append("COUNTS               NUMBER(18) ,")
      .append("SECONDS              NUMBER(18) ,")
      .append("BILL_MINUTE          NUMBER(18) ,")
      .append("SETTLE_MIN           NUMBER(18) ,")
      .append("INTRUNK_CARRIER_ID   NUMBER(5)  NOT NULL,")
      .append("OUTTRUNK_CARRIER_ID  NUMBER(5)  NOT NULL,")
      .append("ROUTE_ID             NUMBER(10) NOT NULL,")
      .append("PRE_FEE              NUMBER(18) ,")
      .append("FEE                  NUMBER(18) ,")
      .append("RECEIVE_DAY          NUMBER(8)  NOT NULL,")
      .append("DESTINATION_ID       NUMBER(10) ")
      .append(" )")
    sql.toString()
  }
  def test(spark:SparkSession):DataFrame={
    val rdd =spark.read.parquet("/user/hive/warehouse/grouped_cdr_201806/*")
    import spark.implicits._
    //spark.createDataFrame(rdd.rdd,getType())
    spark.createDataFrame(rdd.rdd,getType())
  }
  def getType():StructType={
    StructType(
      StructField("call_year"           ,DecimalType(4,0)  ,false)::
        StructField("call_month"          ,DecimalType(2,0) ,false  )::
        StructField("call_day"            ,DecimalType(2,0) ,false  )::
        StructField("call_hour"           ,DecimalType(2,0) ,false  )::
        StructField("caller_country_id"   ,DecimalType(5,0) ,true  )::
        StructField("caller_province_id"  ,DecimalType(5,0) ,true )::
        StructField("caller_zone"         ,DecimalType(5,0),true  )::
        StructField("caller_carrier_id"   ,DecimalType(5,0),true  )::
        StructField("caller_type"         ,StringType    ,true    )::
        StructField("callee_type"         ,StringType      ,true  )::
        StructField("callee_carrier_id"   ,DecimalType(5,0) ,true )::
        StructField("callee_zone"         ,DecimalType(5,0) ,true )::
        StructField("callee_province_id"  ,DecimalType(5,0) ,true )::
        StructField("callee_country_id"   ,DecimalType(5,0) ,true )::
        StructField("service_type"        ,DecimalType(5,0)  ,false )::
        StructField("dir"                 ,DecimalType(5,0)  ,false )::
        StructField("route_type"          ,StringType  ,true        )::
        StructField("switch_id"           ,DecimalType(5,0)  ,false)::
        StructField("counts"              ,DecimalType(18,0) ,false)::
        StructField("seconds"             ,DecimalType(18,0),false )::
        StructField("bill_min"            ,DecimalType(18,0) ,false)::
        StructField("intrunk_carrier_id"  ,DecimalType(5,0) ,false )::
        StructField("outtrunk_carrier_id" ,DecimalType(5,0)  ,false)::
        StructField("route_id"            ,DecimalType(10,0) ,false)::
        StructField("day_no"              ,StringType        ,false)::
        StructField("pre_fee"             ,DecimalType(18,0),true )::
        StructField("fee"                 ,DecimalType(18,0) ,true)::
        StructField("batch"               ,DecimalType(10,0),false )::
        StructField("destination_id"      ,DecimalType(10,0),true )::
        StructField("account_date"        ,DecimalType(8,0) ,false )::
        StructField("settle_flag"         ,DecimalType(1,0) ,false )::
        StructField("process_date"        ,DecimalType(8,0) ,false )::
        StructField("settle_month"        ,DecimalType(10,0) ,true)::
        Nil
    )
  }
}