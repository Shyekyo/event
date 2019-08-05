package ideal.TS

import org.apache.spark.sql.SparkSession

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
    val fileRdd = spark.sparkContext.textFile("/data/detail_cdr_bur1_go_201811.csv",22)
      .map(line => {
        val arr = line.split(",")
        val sb = new StringBuilder()
        for(pro <- arr){
          sb.append(pro.trim).append(",")
        }
        sb.substring(0,sb.length-1).toString()
      })
    fileRdd.saveAsTextFile("/data/trim")
  }
}
