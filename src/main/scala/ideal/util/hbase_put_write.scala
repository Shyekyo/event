package ideal.util

import ideal.constants.Constants
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/8/5.
  */
object hbase_put_write {

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
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)


    val indataRDD = spark.sparkContext.makeRDD(Array("002,10","003,10","004,50"))

    indataRDD.foreachPartition(x=> {
      val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
      conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

      val htable = HBaseUtils.getTable(conf,tableName)

      x.foreach(y => {
        val arr = y.split(",")
        val key = arr(0)
        val value = arr(1)

        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("info"),Bytes.toBytes("clict_count"),Bytes.toBytes(value))
        htable.put(put)
      })
    })

    spark.stop()

  }
}
