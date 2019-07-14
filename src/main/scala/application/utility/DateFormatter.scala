package application.utility

import java.sql.Timestamp
import org.joda.time.format.DateTimeFormat

object DateFormatter {

  val formatterStandard = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")

  val formatterStandardNoMS = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss")

  val formatterOutputFile = DateTimeFormat.forPattern("yyyyMMdd")

  def formatToTimestamp(date: String) = {
    try {
      new Timestamp(DateFormatter.formatterStandard.parseMillis(date))
    } catch {
      case e: IllegalArgumentException => new Timestamp(DateFormatter.formatterStandardNoMS.parseMillis(date))
    }
  }

  def formatToDatetime(date: String) = {
    try {
      DateFormatter.formatterStandard.parseDateTime(date)
    } catch {
      case e: IllegalArgumentException => DateFormatter.formatterStandardNoMS.parseDateTime(date)
    }
  }
}
