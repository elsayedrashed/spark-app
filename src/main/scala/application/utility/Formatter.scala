package application.utility

import java.sql.Date
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Formatter {

  private val DATALAKE_DATE_FORMAT = "yyyy-MM-dd"
  private val DATALAKE_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"

  private val formatterStandardMS: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
  private val formatterStandard: DateTimeFormatter = DateTimeFormat.forPattern(DATALAKE_DATETIME_FORMAT)
  private val formatterStandardDate: DateTimeFormatter = DateTimeFormat.forPattern(DATALAKE_DATE_FORMAT)

  private val formatterNaturalDate: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy")
  private val formatterNaturalDateTimeMinutes: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm")
  private val formatterNaturalDateTimeSecondes: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss")
  private val formatterNaturalDateTimeSecondesAm: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy hh:mm:ss a")
  private val formatterNaturalDateTimeMillis: DateTimeFormatter = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss.SSS")

  private val formatterNaturalDateUSA: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")
  private val formatterNaturalDateTimeMinutesUSA: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm")
  private val formatterNaturalDateTimeSecondesUSA: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss")
  private val formatterNaturalDateTimeSecondesUSAAm: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy hh:mm:ss a")
  private val formatterNaturalDateTimeMillisUSA: DateTimeFormatter = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
  private val formatterVerboseDate: DateTimeFormatter = DateTimeFormat.forPattern("dd-MMM-yy")

  def toDate(date: String): Option[Date] = {
    if (date == null || date.isEmpty) {
      return None
    }
    val patterns = List(
      formatterStandardMS, formatterStandard, formatterStandardDate,
      formatterNaturalDateTimeMillis, formatterNaturalDateTimeSecondes, formatterNaturalDateTimeSecondesAm, formatterNaturalDateTimeMinutes, formatterNaturalDate,
      formatterNaturalDateTimeMillisUSA, formatterNaturalDateTimeSecondesUSA, formatterNaturalDateTimeSecondesUSAAm, formatterNaturalDateTimeMinutesUSA, formatterNaturalDateUSA,
      formatterVerboseDate
    )
    for (pattern: DateTimeFormatter <- patterns) {
      try {
        return Some(new Date(pattern.parseDateTime(date).getMillis))
      } catch {
        case _: IllegalArgumentException =>
      }
    }
    None
  }

  def toDateStr(date: Option[Date]): String = {
    if (date == null || date.isEmpty) {
      return null
    }
    DateTimeFormat.forPattern(DATALAKE_DATE_FORMAT).print(new DateTime(date.get))
  }

  def toDateStr(date: Date): String = {
    if (date == null) {
      return null
    }
    DateTimeFormat.forPattern(DATALAKE_DATE_FORMAT).print(new DateTime(date))
  }

  def toDateTimeStr(date: Option[Date]): String = {
    if (date == null || date.isEmpty) {
      return null
    }
    DateTimeFormat.forPattern(DATALAKE_DATETIME_FORMAT).print(new DateTime(date.get))
  }

  def toDateTimeStr(date: Date): String = {
    if (date == null) {
      return null
    }
    DateTimeFormat.forPattern(DATALAKE_DATETIME_FORMAT).print(new DateTime(date))
  }


}
