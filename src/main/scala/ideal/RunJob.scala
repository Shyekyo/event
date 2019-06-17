package ideal

import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/6/17.
  */
object RunJob {
  case class model(a:String,b:String,c:String,d:String,e:String,f:String)
  def main(args: Array[String]): Unit = {
    val logFile = "wx"
    val file = RunJob.getClass.getClassLoader.getResource(logFile).getPath
    val spark = SparkSession.builder().
      appName("xsql").
      master("local[1]").
      config("spark.sql.warehouse.dir", "C:/Users/SongHyeKyo/ideaProjects/event/spark-warehouse").
      getOrCreate()
    val textRdd = spark.sparkContext.textFile(file).
      map({
        line =>
          val arr = line.split("\t")
          model(arr(0),arr(1),arr(2),arr(3),arr(4),arr(5))
      }).
      toJavaRDD()
    val wechat = spark.createDataFrame(textRdd)
    wechat.createOrReplaceTempView("wechat")
    val df = spark.sql("select * from wechat")
    df.filter({ row =>
      var flag=false
      if(row.get(0)=="2018082713"){
        flag=true
      }
      flag
    })
    df.show()
  }
}
