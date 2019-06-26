package ideal.constants

/**
  * Created by zhangxiaofan on 2019/6/24.
  */
object Constants {
  val ORACLE_JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver"
  val ORACLE_URL : String= "jdbc:oracle:thin:@//192.9.100.204:1521/ORCL"
  val ORACLE_USER: String= "ivoss"
  val ORACLE_PASSWORD : String= "Orivoss_m_204"
  val SPARK_APP_MASTER_LOCAL : String = "local[*]"
  val SPARK_APP_YARN : String = "yarn-client"
}
