package ideal.TS

import ideal.constants.Constants
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/7/4.
  */
object hhs {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      master(Constants.SPARK_APP_YARN).enableHiveSupport()
      //config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val sql = "select count(*) from DETAIL_CDR_BUR"
    val df = spark.sql(sql)
    df.show()
    df.foreach(print(_))
  }
}
