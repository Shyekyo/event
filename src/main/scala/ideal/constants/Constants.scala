package ideal.constants

/**
  * Created by zhangxiaofan on 2019/6/24.
  */
object Constants {
  val ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver"
  val ORACLE_IVOSS_URL : String= "jdbc:oracle:thin:@//192.9.100.204:1521/ORCL"
  val ORACLE_IVOSS_USER: String= "ivoss"
  val ORACLE_IVOSS_PASSWORD : String= "Orivoss_m_204"
  val SPARK_APP_MASTER_LOCAL : String = "local[*]"
  val SPARK_APP_YARN : String = "yarn-client"

  val ORACLE_CTGIRM_URL : String= "jdbc:oracle:thin:@//192.9.100.205:1521/ORCL"
  val ORACLE_CTGIRM_URL_2 : String= "jdbc:oracle:thin:@//192.9.100.205:1521/ORCL"
  //"jdbc:oracle:thin:@//ip:1521/dbinstance"
  val ORACLE_CTGIRM_USER: String= "ctgirm"
  val ORACLE_CTGIRM_PASSWORD : String= "Ctgtest_m_205"

  val MYSQL_URL : String = "jdbc:mysql://192.9.100.105:3306/test"
  val MySQL_JDBC_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_USER: String= "root"
  val MYSQL_PASSWORD : String= "Bighead2019;"
}
