package ideal.util

import ideal.constants.Constants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/8/5.
  */
object hbase_read {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName(this.getClass.getSimpleName).
      master(Constants.SPARK_APP_MASTER_LOCAL).
      enableHiveSupport().
      config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      //config("spark.sql.warehouse.dir", prefix2+"spark-warehouse").
      .getOrCreate()
    import spark.implicits._

    val tableName = "table_name"
    val quorum = "zk"
    val port = "2181"

    // 配置相关信息
    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    // HBase数据转成RDD
    val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).cache()

    // RDD数据操作
    val data = hBaseRDD.map(x => {
      val result = x._2
      val row = result.getRow
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("info".getBytes,"content".getBytes))
      (key,value)
    })

    data.foreach(println)

    spark.stop()
  }
}
