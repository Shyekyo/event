package ideal.TS

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark._

import scala.io.Codec

/**
  * Created by zhangxiaofan on 2019/7/24.
  */
object etl {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName)
      //master(Constants.SPARK_APP_YARN)
      .enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    val fileRdd = spark.sparkContext.textFile(args(0))
      .map(line => {
        val arr = line.split(",")
        val sb = new StringBuilder()
        for(pro <- arr){
          sb.append(pro.trim).append(",")
        }
        sb.substring(0,sb.length-1).toString()
      })
    fileRdd.coalesce(1).saveAsTextFile(args(1))
  }
}
