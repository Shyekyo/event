package ideal
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * Created by zhangxiaofan on 2019/8/23.
  */
object join {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    val carrier = getJoinAttrs("carrier")
    val carrier_code = getJoinAttrs("carrier_code")
    val country = getJoinAttrs("country")
    val sell_code = getJoinAttrs("sell_code")
    val service_type_class = getJoinAttrs("service_type_class")
    val service_type_item = getJoinAttrs("service_type_item")
    val switch_config = getJoinAttrs("switch_config")
    readTable(spark,carrier,"carrier")
    readTable(spark,carrier_code,"carrier_code")
    readTable(spark,country,"country")
    readTable(spark,sell_code,"sell_code")
    readTable(spark,service_type_class,"service_type_class")
    readTable(spark,service_type_item,"service_type_item")
    readTable(spark,switch_config,"switch_config")
    spark.read.parquet("/detial").createOrReplaceTempView("detail")

  }
  def readTable(spark:SparkSession,table:String*): Unit ={
    val readDf = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.9.100.105:3306/test")
      .option("user", "root").option("password","Bighead2019;")
      .option("dbtable", table(0))
      if(table.equals("carrier_code")) {
        readDf.option("partitionColumn", "callee_type")
          .option("lowerBound", 0)
          .option("upperBound", 3)
          .option("numPartitions", 3)
      }
      readDf.option("fetchsize", 500).load().toDF().createOrReplaceTempView(table(1))
  }

  def getJoinAttrs(table:String): String = table match {
    case "carrier" => "(select carrier_id,carrier_ename from carrier) b"
    case "carrier_code" => "(select destination_name,destination_id,effect_date,expire_date,country_id from carrier_code) a"
    case "country" => "(select country_id,english_name from country) a"
    case "sell_code" => "(select destination_name,destination_id,bureau_id,effect_date,expire_date from sell_code) a"
    case "service_type_class" => "(select type_class_id,type_class_name from service_type_class) a"
    case "service_type_item" => "(select type_id,type_class_id from service_type_item) a"
    case "switch_config" => "(select switch_id,switch_name from switch_config) a"
    case _ => "-1"
  }
}
