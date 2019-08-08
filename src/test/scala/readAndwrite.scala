import ideal.constants.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._
import org.json4s.jackson.Json



/**
  * Created by zhangxiaofan on 2019/8/6.
  */
object readAndwrite {
  case class CDR(dir:Int,route_type:Int,switch_id:Int,counts:Int,seconds:Int,account_date:Int)
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
    val resource ="""cdrindex/_doc/"""
    var query = "{\"query\":{\"range\":{\"dir\":{\"gte\":1 ,\"lte\":" +3+ "}}}}"
    query = "{\"query\":{\"match\":{\"dir\":1}}}"

    query = "{\"query\":{\"bool\":{\"must\":{\"match\":{\"dir\":1}},\"filter\":{\"range\":{\"seconds\":{\"gte\":40}}}}}}"//{\"match\":{\"dir\":1}}}"
   //{"query":"bool":{"must":[{"match":{"dir":1}}],"filter":[{"range":{"seconds":{"gte":40}}}]}}
    //array = spark.sparkContext.esJsonRDD("cdrindex/_doc","?q=*")
    val array = spark.sparkContext.esJsonRDD(resource,query)
      .foreachPartition(_.foreach(println _))
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
