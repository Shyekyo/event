package ideal

import ideal.Int_fix_month_sum.{p_acct_data_class, p_bureau_id, p_cycle_id}
import ideal.constants.Constants
import ideal.util.SparkUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.{Codec, Source}

/**
  * Created by zhangxiaofan on 2019/7/24.
  */
object tmp_load_oracle {

  def main(args: Array[String]): Unit = {

  }
//95.46464646464646
  def loadOracle(partition:Int):Unit={
    val v_month=201806
    //Logger.getLogger("org").setLevel(Level.WARN)
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse").
      getOrCreate()

    import spark.implicits._
    val groupedCdr = s"grouped_cdr_$v_month"
    //val stt_object ="stt_object"
    val sql_str = s"(select * from $groupedCdr) b"
    val table = spark.read
      .format("jdbc")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .option("url", Constants.ORACLE_CTGIRM_URL)
      .option("user", Constants.ORACLE_CTGIRM_USER)
      .option("password", Constants.ORACLE_CTGIRM_PASSWORD)
      .option("dbtable", sql_str)
      .option("fetchsize", 5000)
      .load()
    /*table
      .repartition(100)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(groupedCdr)*/
    table.repartition(partition)
      .write
      .csv("/data/tmp")
  }
}
