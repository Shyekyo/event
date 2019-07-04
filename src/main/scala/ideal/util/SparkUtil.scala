package ideal.util

import java.util.Properties

import ideal.Rep_total_account_commit1.{_logging, getOracleProps}
import ideal.constants.Constants
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by zhangxiaofan on 2019/6/27.
  */
object SparkUtil {

  def loadDataFromOracle(spark:SparkSession,url:String,tableName:String,props:Properties):DataFrame={
    spark.read.format("jdbc").jdbc(url,tableName,props)
    /*.option("url",url)
     .option("driver","oracle.jdbc.driver.OracleDriver")
     .option("dbtable",table)
     .option("user","ivoss")
     .option("password","Orivoss_m_204").load()*/
  }

  def getOracleIvossProps():Properties={
    val props = new Properties()
    props.put("user", Constants.ORACLE_IVOSS_USER)
    props.put("password", Constants.ORACLE_IVOSS_PASSWORD)
    props.put("driver", Constants.ORACLE_JDBC_DRIVER)
    props
  }

  def getOracleCtgirmProps():Properties={
    val props = new Properties()
    props.put("user", Constants.ORACLE_CTGIRM_USER)
    props.put("password", Constants.ORACLE_CTGIRM_PASSWORD)
    props.put("driver", Constants.ORACLE_JDBC_DRIVER)
    props
  }

  def writeDataIntoOrcale(df: DataFrame, TableName: String,destation:Int=204): Unit = {
    var props :Properties= null
    var url :String= null
    if(destation==204){
      props = getOracleIvossProps()
      url = Constants.ORACLE_IVOSS_URL
    }else if(destation==205){
      props = getOracleCtgirmProps()
      url = Constants.ORACLE_CTGIRM_URL
    }
    df.write.mode(SaveMode.Append)
      .jdbc(
        url,
        TableName,
        props
      )
    _logging.info(s"save table $TableName is ok !")
  }
}
