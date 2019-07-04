package ideal.util

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import ideal.Int_fix_month_sum.p_cycle_id
import ideal.constants.Constants
import ideal.util.DBUtil.{Close, init204}
import ideal.util.SparkUtil.getOracleIvossProps
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * Created by zhangxiaofan on 2019/6/24.
  */
object DBUtil {
  val _logging = Logger.getLogger(DBUtil.getClass)
  var conn: Connection = null
  var statement: Statement = null

  def insertOra(sql:String,destation:Int=204):Unit ={
    if(destation==204){
      init204()
    }else if(destation==205){
      init204()
    }
    addSQL(sql)
    Close()
  }


  def queryOra(sql: String,destation:Int=204): Array[Array[String]] = {
    val table = ArrayBuffer[Array[String]]()
    try {
      if(destation==204){
        init204()
      }else if(destation==205){
        init204()
      }
      _logging.info("执行的SQL：\n" + sql + "\n")
      val rs = statement.executeQuery(sql)
      while (rs.next()) {
        val row = getTuple(rs)
        table.append(row)
      }
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
    table.toArray
  }

  private def getTuple(rs: ResultSet): Array[String] = {
    val tempArray = ArrayBuffer[String]()
    try {
      val columnCount = rs.getMetaData.getColumnCount

      for (i <- 1 to columnCount) {
        tempArray.+=(rs.getString(i))
      }
    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
    tempArray.toArray
  }

  def clearTable(tableName:String,destation:Int=204):Unit = {
    try {
      if (destation == 204) {
        init204()
      } else if (destation == 205) {
        init204()
      }
      val sql = s"truncate table ${tableName}"
      _logging.info("执行的SQL：\n" + sql + "\n")
      statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
  }
  def delTable(tableName:String,destation:Int=204):Unit ={
    try {
      if(destation==204){
        init204()
      }else if(destation==205){
        init204()
      }
      val sql = "drop table " + tableName
      _logging.info("执行的SQL：\n" + sql + "\n")
      statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
  }
  private def init204(): Unit = {
    val driver = Constants.ORACLE_JDBC_DRIVER
    val url = Constants.ORACLE_IVOSS_URL
    val username = Constants.ORACLE_IVOSS_USER
    val password = Constants.ORACLE_IVOSS_PASSWORD
    Class.forName(driver)
    conn = DriverManager.getConnection(url, username, password)
    statement = conn.createStatement()
  }

  private def init205(): Unit = {
    val driver = Constants.ORACLE_JDBC_DRIVER
    val url = Constants.ORACLE_CTGIRM_URL
    val username = Constants.ORACLE_CTGIRM_USER
    val password = Constants.ORACLE_CTGIRM_PASSWORD
    Class.forName(driver)
    conn = DriverManager.getConnection(url, username, password)
    statement = conn.createStatement()
  }

  def addSQL(sql:String,destation:Int=204):Unit ={
    _logging.info("执行的SQL：\n" + sql + "\n")
    try {
      if(destation==204){
        init204()
      }else if(destation==205){
        init204()
      }
      statement.execute(sql)
    } catch {
      case  e:Exception => e.printStackTrace()
    } finally {
      Close()
    }
  }

  def Close(): Unit = {
    statement.close()
    conn.close()
  }

  def createTable(sql:String,dropOldtableName:String,falg:Boolean=false,destation:Int=204):Unit = {
    try {
      if(destation==204){
        init204()
      }else if(destation==205){
        init204()
      }
      _logging.info("执行的SQL：\n" + sql + "\n")
      if(falg){
        statement.execute(s"drop table $dropOldtableName")
      }
      statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
  }

  def tableExists(tableName:String,destation:Int=204):Boolean ={
    Try {
      try {
        if(destation==204){
          init204()
        }else if(destation==205){
          init204()
        }
        val sql = getTableExistsQuery(tableName)
        _logging.info("执行的SQL：\n" + sql + "\n")
        val stat = conn.prepareStatement(sql)
        stat.executeQuery()
      } catch {
        case ex: Exception => ex.printStackTrace()
      } finally {
        Close()
      }
    }.isSuccess
  }

  private def getTableExistsQuery(table: String): String = {
    s"SELECT * FROM $table WHERE 1=0"
  }

  def main(args: Array[String]): Unit = {

  }
}
