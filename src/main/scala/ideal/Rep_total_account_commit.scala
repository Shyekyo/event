package ideal

import ideal.CaseClass
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangxiaofan on 2019/6/21.
  */
object Rep_total_account_commit {
  def main(args: Array[String]): Unit = {
    val account_type = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\account_type.csv"
    val acct_item = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\acct_item.csv"
    val acct_item_type = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\acct_item_type.csv"
    val amount = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\amount.csv"
    val billing_cycle = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\billing_cycle.csv"
    val gen_item = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\gen_item.csv"
    val stt_object = "D:\\GITRepo\\event\\src\\resources\\rep_total_account_commit\\stt_object.csv"

    val spark = SparkSession.builder().
      appName("xsql").
      master("local[1]").
      config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse").
      getOrCreate()
    import spark.implicits._
    val ccl = new CaseClass
    //val account_type_df = spark.read.csv(account_type).as[ccl.ACCOUNT_TYPE]
    //val acct_item_df = spark.read.csv(acct_item).as[ccl.ACCT_ITEM]
    //val acct_item_type_df = spark.read.csv(acct_item_type).as[ccl.ACCT_ITEM_TYPE]
    //val amount_df = spark.read.csv(amount).as[ccl.AMOUNT]
    //val billing_cycle_df = spark.read.csv(billing_cycle).as[ccl.BILLING_CYCLE]
    //val gen_item_df = spark.read.csv(gen_item).as[ccl.GEN_ITEM]
    //val stt_object_df = spark.read.csv(stt_object).as[ccl.STT_OBJECT]
    val sc = spark.sparkContext
    val account_type_df = sc.textFile(account_type).map(
      line => {
      val arr = line.split(",")
      ccl.ACCOUNT_TYPE(arr(0).toInt,arr(1),arr(2),arr(3))
    }).toDS()
    val acct_item_df = sc.textFile(acct_item).toDF()
    val acct_item_type_df = sc.textFile(acct_item_type).toDF()
    val amount_df = sc.textFile(amount).toDF()
    val billing_cycle_df = sc.textFile(billing_cycle).toDF()
    val gen_item_df = sc.textFile(gen_item).toDF()
    val stt_object_df = sc.textFile(stt_object).toDF()
    account_type_df.createOrReplaceTempView("account_type")
    acct_item_df.createOrReplaceTempView("acct_item")
    acct_item_type_df.createOrReplaceTempView("acct_item_type")
    amount_df.createOrReplaceTempView("amount")
    billing_cycle_df.createOrReplaceTempView("billing_cycle")
    gen_item_df.createOrReplaceTempView("gen_item")
    stt_object_df.createOrReplaceTempView("stt_object")

    spark.sql("select * from account_type").show()


  }
}
