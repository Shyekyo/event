package ideal

import ideal.constants.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by zhangxiaofan on 2019/8/22.
  */
object checkCDR {
  //val _logging = Logger.getLogger(checkCDR.getClass)
  //Logger.getLogger("org").setLevel(Level.WARN)
  case class DETAIL(caller_num:String,caller_zone:Int,caller_province_id:Int,caller_country_id:Int,caller_type:Int,
                    caller_carrier_id:Int,callee_num:String,callee_zone:Int,callee_province_id:Int,
                    callee_country_id:Int,callee_type:Int,callee_carrier_id:Int,outtrunk_carrier_id:Int,
                    intrunk_carrier_id:Int,out_trunk:String,in_trunk:String,call_year:Int,call_month:Int,
                    call_day:Int,call_hour:Int,call_min:Int,duration:Int,service_type:Int,route_type:Int,
                    switch_id:Int,file_id:String,direction:Int,route_id:Int,pre_fee:Int,fee:Int,calls:Int,
                    destination_id:Int,batchid:Long,third_num:String,rec_type:String,bearer_service:String,
                    trans_param:String,sell_dest_id:String,breau_destination_id:Int,ca_dest_id:Int,settle_month:Int,
                    orig_start_time:String)
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
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //.config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    getFileTest(spark: SparkSession)
    spark.stop()
  }

  def getFileTest(spark: SparkSession): Boolean ={
    import spark.implicits._
    var bool:Boolean=false;
    val original = spark.sparkContext.textFile("/20190815")
      .map(line => line.split(","))
      .map(attr => DETAIL(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
        , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
        , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16).toInt, attr(17).toInt
        , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21).toInt, attr(22).toInt, attr(23).toInt
        , attr(24).toInt, attr(25), attr(26).toInt, attr(27).toInt, attr(28).toInt, attr(29).toInt
        , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33), attr(34), attr(35)
        , attr(36), attr(37), attr(38).toInt, attr(39).toInt, attr(40).toInt, attr(41))).toDS()
    val transfer = spark.read.parquet("/parquet")
      .select($"direction",$"intrunk_carrier_id",$"duration")
    val oDurationSumAndCount = original
      .select($"duration")
      .agg("duration" -> "sum", "duration" -> "count").toDF("sum","count")
    val tDurationSumAndCount = transfer
      .select($"duration")
      .agg("duration" -> "sum", "duration" -> "count").toDF("sum","count")
    val count1 = oDurationSumAndCount.except(tDurationSumAndCount).count
    val result1 = original.groupBy($"direction",$"intrunk_carrier_id")
      .agg("duration" -> "sum","duration" ->"count")
    val result2 = transfer.groupBy($"direction",$"intrunk_carrier_id")
      .agg("duration" -> "sum","duration" ->"count")
    //val total = result1.intersect(result2)//交集
    val count2 =result1.except(result2).count
    if(count1==0 && count2==0) {
      bool=true
    }
    bool
  }

  def matchDirection(direction: Int,attr :Array[String]): String = direction match {
    case 1 => attr(13)
    case 2 => attr(12)
    case 3 => attr(12)
    case 4 => attr(13)
    case _ => "-1"
  }
  def getTotalTest(spark: SparkSession): Boolean ={
    import spark.implicits._
    var bool:Boolean=false;
    val original = spark.sparkContext.textFile("/org").map(line => line.split(",")).map(attr =>

      DETAIL2(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
        , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
        , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16), attr(17).toInt
        , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21), attr(22).toInt, attr(23).toInt
        , attr(24), attr(25).toInt, attr(26), attr(27), attr(28).toInt, attr(29).toInt
        , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33).toInt, attr(34).toInt, attr(35).toInt
        , attr(36).toInt, attr(37).toInt, attr(38), attr(39), attr(40).toInt, attr(41))).toDS()
    val transfer = spark.read.parquet("/detail")
      .select($"direction",$"intrunk_carrier_id",$"duration")
    val oDurationSumAndCount = original
      .select($"duration")
      .agg("duration" -> "sum", "duration" -> "count").toDF("sum","count")
    val tDurationSumAndCount = transfer
      .select($"duration")
      .agg("duration" -> "sum", "duration" -> "count").toDF("sum","count")
    val count1 = oDurationSumAndCount.except(tDurationSumAndCount).count
    val result1 = original.groupBy($"direction",$"intrunk_carrier_id")
      .agg("duration" -> "sum","duration" ->"count")
    val result2 = transfer.groupBy($"direction",$"intrunk_carrier_id")
      .agg("duration" -> "sum","duration" ->"count")
    //val total = result1.intersect(result2)//交集
    val count2 =result1.except(result2).count
    if(count1==0 && count2==0) {
      bool=true
    }
    bool
  }


  /* val original = spark.sparkContext.textFile(path).map(line => line.split(",")).map(attr => DETAIL(attr(0), attr(1).toInt, attr(2).toInt, attr(3).toInt, attr(4).toInt, attr(5).toInt
       , attr(6), attr(7).toInt, attr(8).toInt, attr(9).toInt, attr(10).toInt, attr(11).toInt
       , attr(12).toInt, attr(13).toInt, attr(14), attr(15), attr(16).toInt, attr(17).toInt
       , attr(18).toInt, attr(19).toInt, attr(20).toInt, attr(21).toInt, attr(22).toInt, attr(23).toInt
       , attr(24).toInt, attr(25), attr(26).toInt, attr(27).toInt, attr(28).toInt, attr(29).toInt
       , attr(30).toInt, attr(31).toInt, attr(32).toInt, attr(33), attr(34), attr(35)
       , attr(36), attr(37), attr(38).toInt, attr(39).toInt, attr(40).toInt, attr(41)))*/
  def toMysqlStruct(spark:SparkSession): Unit ={
    val struct = StructType(
      StructField("carrier_id",IntegerType,false)::
        StructField("iss_id",IntegerType,true)::
        StructField("carrier_name",StringType,true)::
        StructField("carrier_ename",StringType,false)::
        StructField("country",IntegerType,false)::
        StructField("is_direct",IntegerType,false)::
        StructField("contact_person",StringType,true)::
        StructField("telephone_no",StringType,true)::
        StructField("fax_no",StringType,true)::
        StructField("email_address",StringType,true)::
        StructField("scale",StringType,true)::
        StructField("address",StringType,true)::
        StructField("bank",StringType,true)::
        StructField("trans_bank",StringType,true)::
        StructField("bank_account",StringType,true)::
        StructField("settle_cycle",IntegerType,true)::
        StructField("settle_date",IntegerType,true)::
        StructField("corp_name",StringType,true)::
        StructField("swift",StringType,true)::
        StructField("beneficiary_name",StringType,true)::
        StructField("bank_addr",StringType,true)::
        StructField("trans_bank_addr",StringType,true)::
        StructField("trans_bank_account",StringType,true)::
        StructField("connect_person_title",StringType,true)::
        StructField("connect_person_post",StringType,true)::
        StructField("edi_code",StringType,true)::
        StructField("memo",StringType,true)::
        StructField("update_time",TimestampType,false)::
        StructField("is_import",IntegerType,true)::
        StructField("update_by",StringType,true)::
        StructField("customer_email",StringType,true)::
        StructField("route_class",IntegerType,true)::
        StructField("category_id",IntegerType,true)::
        StructField("customer_name",StringType,true)::
        StructField("email_subject",StringType,true)::
        StructField("supplier_abb",StringType,true)::
        StructField("carrier_am",StringType,true)::
        StructField("ct_am",StringType,true)::
        Nil
    )
    val org = spark.sparkContext.textFile("/dim/csv").map(
      line =>{
        val arr = line.split(",")
        Row.fromSeq(arr)
      }
    )
    val frame = spark.createDataFrame(org,struct)
    val df = spark.read.csv("/dim/carrier.csv").toDF("carrier_id","iss_id","carrier_name","carrier_ename","country","is_direct","contact_person","telephone_no","fax_no","email_address","scale","address","bank","trans_bank","bank_account","settle_cycle","settle_date","corp_name","swift","beneficiary_name","bank_addr","trans_bank_addr","trans_bank_account","connect_person_title","connect_person_post","edi_code","memo","update_time","is_import","update_by","customer_email","route_class","category_id","customer_name","email_subject","supplier_abb","carrier_am","ct_am")

  }
}
