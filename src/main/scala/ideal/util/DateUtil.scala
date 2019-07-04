package ideal.util

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by zhangxiaofan on 2019/6/27.
  */
object DateUtil {

  def getToday(): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss")
    val cal: Calendar = Calendar.getInstance()
    val today = dateFormat.format(cal.getTime())
    today
  }

  def main(args: Array[String]): Unit = {
    val str = getToday()
    print(str)
  }
}
