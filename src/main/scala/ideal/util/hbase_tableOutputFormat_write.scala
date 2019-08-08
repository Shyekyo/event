package ideal.util

import ideal.constants.Constants
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/8/5.
  */
object hbase_tableOutputFormat_write {
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

    /*val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("20180325#1001")))
    scan.setFilter(filter)*/
    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val table = HBaseUtils.getTable(conf,tableName)

    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputKeyClass(classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoadMap(job, table)


    // inputRDD data
    val indataRDD = spark.sparkContext.makeRDD(Array("20180723_02,13","20180723_03,13","20180818_03,13"))
    val rdd = indataRDD.map(x => {
      val arr = x.split(",")
      val kv = new KeyValue(Bytes.toBytes(arr(0)),"info".getBytes,"content".getBytes,arr(1).getBytes)
      (new ImmutableBytesWritable(Bytes.toBytes(arr(0))),kv)
    })

    // 保存Hfile to HDFS
    rdd.saveAsNewAPIHadoopFile("hdfs://localhost:8020/tmp/hbase",classOf[ImmutableBytesWritable],classOf[KeyValue],classOf[HFileOutputFormat2],conf)

    // Bulk写Hfile to HBase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://localhost:8020/tmp/hbase"),table)

    spark.stop()
  }
}
