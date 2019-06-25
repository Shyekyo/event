package ideal

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhangxiaofan on 2019/6/24.
  */
object DBUtil {
  val _logging = Logger.getLogger(DBUtil.getClass)
  var conn: Connection = null
  var statement: Statement = null

  def insertOra(sql:String):Unit ={
    init()
    addSQL(sql)
    Close()
  }

  def queryOra(sql: String): Array[Array[String]] = {
    val table = ArrayBuffer[Array[String]]()
    try {
      init()
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

  def delTable(tableName:String):Unit ={
    try {
      init()
      val sql = "drop table " + tableName
      _logging.info("执行的SQL：\n" + sql + "\n")
      statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
  }

  def init(): Unit = {
    val driver = Constants.ORACLE_JDBC_DRIVER
    val url = Constants.ORACLE_URL
    val username = Constants.ORACLE_USER
    val password = Constants.ORACLE_PASSWORD
    Class.forName(driver)
    conn = DriverManager.getConnection(url, username, password)
    statement = conn.createStatement()
  }

  def addSQL(sql:String):Unit ={
    _logging.info("执行的SQL：\n" + sql + "\n")
    try {
      init()
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

  def createTable(sql:String,tableName:String,falg:Boolean=true):Unit = {
    try {
      init()
      _logging.info("执行的SQL：\n" + sql + "\n")
      if(falg){
        statement.execute("drop table " + tableName)
      }
      statement.execute(sql)
    } catch {
      case ex: Exception => ex.printStackTrace()
    } finally {
      Close()
    }
  }
}
