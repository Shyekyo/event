package ideal.es

import ideal.constants.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

/**
  * Created by zhangxiaofan on 2019/8/6.
  */
object readAndwrite {
  case class CDR(dir:Int,route_type:Int,switch_id:Int,counts:Int,seconds:Int,account_date:Int)
  case class DETAIL(caller_num:String,caller_zone:Int,caller_province_id:Int,caller_country_id:Int,caller_type:Int,
                    caller_carrier_id:Int,callee_num:String,callee_zone:Int,callee_province_id:Int,
                    callee_country_id:Int,callee_type:Int,callee_carrier_id:Int,
                    outtrunk_carrier_id:Int,intrunk_carrier_id:Int,out_trunk:String,
                    in_trunk:String,start_date:String,start_hh:Int,start_miss:Int,duration:Int,
                    service_type:Int,route_type:String,switch_id:Int,file_id:Int,third_num:String,
                    trunk_flag:Int,bearer_service:String,trans_param:String,destination_id:Int,
                    ca_dest_id:Int,sell_dest_id:Int,db_time:Int,s_destination_id:Int,
                    route_id:Int,pre_fee:Int,fee:Int,calls:Int,batchid:Int,rec_type:String,
                    process_time:String,settle_month:Int,orig_start_time:String)
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName)
      //.enableHiveSupport()
      .master(Constants.SPARK_APP_MASTER_LOCAL)
      //config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.warehouse.dir", "D:\\GITRepo\\event\\spark-warehouse")
      .config("es.index.auto.create", "true")
      .config("es.nodes","localhost")
      .config("es.port","9200")
      .getOrCreate()
    import spark.implicits._
    import org.elasticsearch.spark.rdd._

    //val mk = makeCdrRdd(spark)
    val file = spark.sparkContext.textFile("C:\\Users\\SongHyeKyo\\Desktop\\work\\data\\detail_bur1_transgo_201811.csv")
    val mk = file.map(line => {
      val strings = line.split(",")
      DETAIL(strings(0),strings(1).toInt,strings(2).toInt,strings(3).toInt,strings(4).toInt,strings(5).toInt
        ,strings(6),strings(7).toInt,strings(8).toInt,strings(9).toInt,strings(10).toInt,strings(11).toInt
        ,strings(12).toInt,strings(13).toInt,strings(14),strings(15),strings(16),strings(17).toInt
        ,strings(18).toInt,strings(19).toInt,strings(20).toInt,strings(21),strings(22).toInt,strings(23).toInt
        ,strings(24),strings(25).toInt,strings(26),strings(27),strings(28).toInt,strings(29).toInt
        ,strings(30).toInt,strings(31).toInt,strings(32).toInt,strings(33).toInt,strings(34).toInt,strings(35).toInt
        ,strings(36).toInt,strings(37).toInt,strings(38),strings(39),strings(40).toInt,strings(41))
    }
    )
    EsSpark.saveToEs(mk,"cdrindex/_doc")
    //val array = spark.sparkContext.esJsonRDD("cdrindex/_doc","?q=*")
    //val array = spark.sparkContext.esJsonRDD("cdrindex/_doc","{size:1}")
      //.map(_._2)
      //.foreach(println _)
  }

  def makeCdr():Seq[CDR]={
    val one:CDR = CDR(4,2,933,1,24,20180601)
    val two:CDR = CDR(3,1,933,4,202,20180601)
    val third:CDR = CDR(1,1,36,2,60,20180601)
    val four:CDR = CDR(4,2,933,12,1937,20180601)
    val five:CDR = CDR(3,1,933,7,57,20180601)
    val six:CDR = CDR(1,1,35,1,34,20180601)
    val seven:CDR = CDR(2,2,35,1,22,20180601)
    Seq(one,two,third,four,five,six,seven)
  }

  def makeCdrRdd(spark:SparkSession):RDD[readAndwrite.CDR]={
    val mk = spark.sparkContext.makeRDD(makeCdr)
    mk
  }

  def mkexample(spark:SparkSession):RDD[Map[String,Any]]={
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")
    val mk = spark.sparkContext.makeRDD(
      Seq(numbers, airports))
    mk
  }
}
