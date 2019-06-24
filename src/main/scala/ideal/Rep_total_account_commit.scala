package ideal

import org.apache.spark.sql.SparkSession
;

/**
  * Created by zhangxiaofan on 2019/6/21.
  */
object Rep_total_account_commit {
  def main(args: Array[String]): Unit = {
    var prefix = "D:\\GITRepo\\event\\src\\"
    prefix = "C:\\Users\\chendd\\Desktop\\张小凡\\LocalRepo\\event\\src\\"
    val account_type = prefix+"resources\\rep_total_account_commit\\account_type.csv"
    val acct_item = prefix+"resources\\rep_total_account_commit\\acct_item.csv"
    val acct_item_type = prefix+"resources\\rep_total_account_commit\\acct_item_type.csv"
    val amount = prefix+"resources\\rep_total_account_commit\\amount.csv"
    val billing_cycle = prefix+"src\\resources\\rep_total_account_commit\\billing_cycle.csv"
    val gen_item = prefix+"resources\\rep_total_account_commit\\gen_item.csv"
    val stt_object = prefix+"resources\\rep_total_account_commit\\stt_object.csv"
    var prefix2 = "C:\\Users\\chendd\\Desktop\\张小凡\\LocalRepo\\event\\"
    val spark = SparkSession.builder().
      appName("xsql").
      master("local[1]").
      //config("spark.sql.warehouse.dir", "D:/GITRepo/event/spark-warehouse").
      config("spark.sql.warehouse.dir", prefix2+"spark-warehouse").
      getOrCreate()
    import spark.implicits._
    //val account_type_df = spark.read.csv(account_type).as[ccl.ACCOUNT_TYPE]
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
        CaseClass.ACCOUNT_TYPE(arr(0).toInt,arr(1),arr(2),arr(3))
      }).toDS()
    val acct_item_df = sc.textFile(acct_item).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.ACCT_ITEM(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4).toInt, arr(5).toInt,
          arr(6).toInt, arr(7).toInt, arr(8), arr(9).toInt, arr(10).toInt, arr(11), arr(12), arr(13),
          arr(14).toInt, arr(15).toInt, arr(16).toInt, arr(17).toInt, arr(18).toInt)
      }).toDS()
    val acct_item_type_df = sc.textFile(acct_item_type).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.ACCT_ITEM_TYPE(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3), arr(4).toInt, arr(5).toInt,
          arr(6).toInt, arr(7), arr(8), arr(9))
      }).toDS()
    val amount_df = sc.textFile(amount).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.AMOUNT(arr(0).toInt, arr(1).toInt, arr(2).toInt, arr(3).toInt, arr(4), arr(5))
      }).toDS()
    val billing_cycle_df = sc.textFile(billing_cycle).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.BILLING_CYCLE(arr(0).toInt, arr(1).toInt, arr(2).toInt)
      }).toDS()
    val gen_item_df = sc.textFile(gen_item).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.GEN_ITEM(arr(0).toInt, arr(1), arr(2).toInt, arr(3).toInt, arr(4).toInt, arr(5),
          arr(6).toInt, arr(7).toInt, arr(8).toInt, arr(9), arr(10), arr(11), arr(12),
          arr(13).toInt,arr(14).toInt)
      }).toDS()
    val stt_object_df = sc.textFile(stt_object).map(
      line => {
        var rline = line.replace("\"","")
        val arr = rline.split(",")
        CaseClass.STT_OBJECT(arr(0).toInt, arr(1), arr(2).toInt, arr(3).toInt, arr(4).toInt, arr(5).toInt,
          arr(6), arr(7), arr(8), arr(9), arr(10).toInt, arr(11), arr(12),arr(13),arr(14).toInt)
      }).toDS()
    account_type_df.createOrReplaceTempView("account_type")
    acct_item_df.createOrReplaceTempView("acct_item")
    acct_item_type_df.createOrReplaceTempView("acct_item_type")
    amount_df.createOrReplaceTempView("amount")
    billing_cycle_df.createOrReplaceTempView("billing_cycle")
    gen_item_df.createOrReplaceTempView("gen_item")
    stt_object_df.createOrReplaceTempView("stt_object")
    /**
    val sql = "select " +
      "g.gen_id," +
      "g.ref_no," +
      "stt.cust_name," +
      "ait.acct_item_type_id," +
      "ait.name," +
      "bc.cycle_begin_date," +
      "bc.cycle_end_date," +
      "a.amount," +
      "a.currency," +
      "ait.pay_flag," +
      "g.start_date," +
      "g.end_date,decode(decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type), 'TELEPHONE', 1," +
      "decode(decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type), 'TELX', 2," +
      "decode(decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type), 'TELEGRAM', 3," +
      "decode(decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type),'COMMITMENT ADJUST', 5," +
      "decode(decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type),ai.comments, 6, 4))))) sort_id," +
      "decode(sign(instr(ait.name, '调整')), 1, ai.comments, at.acct_type) acct_name," +
      "g.acct_date " +
      "from gen_item g," +
      "acct_item_type ait," +
      "acct_item ai," +
      "amount a," +
      "stt_object stt," +
      "billing_cycle bc," +
      "account_type at" +
      "where " +
      "a.account_item_id = ai.acct_item_id " +
      "and ai.acct_item_type_id = ait.acct_item_type_id " +
      "and ai.gen_id = g.gen_id and g.cust_id = stt.cust_id " +
      "and at.account_type_id =decode(ait.account_type_id, 59, 47, 60, 48, 61, 49, 71, 69, 70, 68, ait.account_type_id)" +
      "and ai.billing_cycle_id = bc.billing_cycle_id " +
      "and g.bureau_id = ai.bureau_id " +
      "and g.bureau_id = bureauId " +
      "and g.ref_no = 1;"
      *
      */
    val sql ="select * from acct_item ai"//,amount a where a.account_item_id = ai.acct_item_id "
    spark.sql(sql).show()
  }
}
