package ideal

import ideal.constants.Constants
import ideal.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.{Codec, Source}

/**
  * Created by zhangxiaofan on 2019/7/24.
  */
object tmp_load_oracle {
  val _logging = Logger.getLogger(tmp_load_oracle.getClass)
  Logger.getLogger("org").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val partition = args(0).toInt
    val tablename = args(1)
    val savepath = args(2)
    val lowerBound = args(3).toInt
    val upperBound = args(4).toInt
    val numPartitions = args(5).toInt
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .enableHiveSupport()
      .master(Constants.SPARK_APP_MASTER_LOCAL)
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    loadOracle(spark,partition,tablename,savepath,lowerBound,upperBound,numPartitions)
  }
//95.46464646464646
  def loadOracle(spark:SparkSession,partition:Int,tableName:String,savepath:String,
                 lowerBound:Int,upperBound:Int,numPartitions:Int):Unit={
    import spark.implicits._
    val sql_str = tableName
    val table = spark
      .read
      .format("jdbc")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("url", Constants.ORACLE_CTGIRM_URL)
      .option("user", Constants.ORACLE_CTGIRM_USER)
      .option("password", Constants.ORACLE_CTGIRM_PASSWORD)
      .option("dbtable", sql_str)
      .option("fetchsize", 10000)
      .option("partitionColumn","start_hh")
      .option("lowerBound",lowerBound)
      .option("upperBound",upperBound)
      .option("numPartitions",numPartitions)
      .load()
    table.repartition(partition)
      .write.mode(SaveMode.Append)
      .parquet(savepath)
    val count = spark.read.parquet(savepath).count()
    _logging.info("count => "+count)
    _logging.info("..SUCCEEDED..")
  }
}
