package application.utility

import java.sql.Date
import org.apache.spark.sql.functions._

object UDF {

  val nvl = udf((left: String, right: String) => if (left == null || left.isEmpty) right else left)

  val concatNullSafe_ = udf((xs: Seq[Any]) => xs.map(item => if (item == null) "" else item).mkString("_"))

  val concatNullSafe__ = udf((xs: Seq[Any]) => xs.map(item => if (item == null) "" else item).mkString("__"))

  val concatNullSafe = udf((xs: Seq[Any], sep: String) => xs.map(item => if (item == null) "" else item).mkString(sep))

  val upper = udf((field: String) => if (field != null) field.toUpperCase else "")

  val nullToNA = udf((field: String) => if (field != null) field else "NA")

  val nullToZero = udf((field: String) => if (field != null) field else "0")

  val toDateStr = udf((date: Date) => {
    if (date == null) null
    else Formatter.toDateStr(date)
  })

  val toDateTimeStr = udf((date: Date) => {
    if (date == null) null
    else Formatter.toDateTimeStr(date)
  })

  val strToDateStr = udf((dateStr: String) => {
    if (dateStr == null) null
    else {
      val date = Formatter.toDate(dateStr)
      Formatter.toDateStr(date)
    }
  })

}
