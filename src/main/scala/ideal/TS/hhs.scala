package ideal.TS

import ideal.constants.Constants
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/7/4.
  */
object hhs {
  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME","hive")
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName)
      .master(Constants.SPARK_APP_MASTER_LOCAL)
      // .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse")
      //config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .getOrCreate()
    import spark.implicits._
    spark.stop()
  }
}
