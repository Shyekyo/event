package ideal.bak

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxiaofan on 2019/6/17.
  */
object RunJob {
  case class model(a:String,b:String,c:String,d:String,e:String,f:String)
  def main(args: Array[String]): Unit = {
    //val file = RunJob.getClass.getClassLoader.getResource(logFile).getPath
    val arr = new ArrayBuffer[String]()
    arr+="d"
    arr+="d"
    arr+="d"

    for(df <- arr){
      println(df)
    }
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
