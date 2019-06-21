package ideal

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by zhangxiaofan on 2019/6/17.
  */
object RunJob {
  case class model(a:String,b:String,c:String,d:String,e:String,f:String)
  def main(args: Array[String]): Unit = {
    val logFile = "D:\\GITRepo\\event\\src\\resources\\wx"
    //val file = RunJob.getClass.getClassLoader.getResource(logFile).getPath
    val spark = SparkSession.builder().
      appName("xsql").
      master("local[1]").
      config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse").
      getOrCreate()
    val textRdd = spark.sparkContext.textFile(logFile).
      map({
        line =>
          val arr = line.split("\t")
          model(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5))
      }).
      toJavaRDD()
    val wechat = spark.createDataFrame(textRdd)
    wechat.createOrReplaceTempView("wechat")
    val df = spark.sql("select * from wechat")
    //df.filter("a=2018082713")
    df.show()
  }

  def WriteDataIntoTable(df: DataFrame, TableName: String): Unit = {
    val props = new Properties()
    props.put("user", "  ")
    props.put("password", "  ")
    props.put("driver", "  ")
    df.write.mode("append")
      .jdbc(
        "url",
        TableName,
        props
      )
  }
}
