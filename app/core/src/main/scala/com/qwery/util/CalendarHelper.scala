package com.qwery.util

import java.time.ZoneId
import java.util.{Calendar, Date, TimeZone}

/**
 * Calendar Helper
 */
object CalendarHelper {

  /**
   * Calendar Utilities
   * @param host the host [[Date date]]
   */
  final implicit class CalendarUtilities(val host: Date) extends AnyVal {

    @inline def dayOfMonth: Int = toCalendar.get(Calendar.DAY_OF_MONTH)

    @inline def dayOfWeek: Int = toCalendar.get(Calendar.DAY_OF_WEEK)

    @inline def hour: Int = toCalendar.get(Calendar.HOUR_OF_DAY)

    @inline def millisecond: Int = toCalendar.get(Calendar.MILLISECOND)

    @inline def minute: Int = toCalendar.get(Calendar.MINUTE)

    @inline def month: Int = toCalendar.get(Calendar.MONTH) + 1

    @inline def second: Int = toCalendar.get(Calendar.SECOND)

    def split: (java.time.LocalDate, java.time.LocalTime) = {
      val date = host.toInstant.atZone(ZoneId.of("GMT")).toLocalDate
      val time = host.toInstant.atZone(ZoneId.of("GMT")).toLocalTime
      (date, time)
    }

    private def toCalendar: Calendar = {
      val cal = Calendar.getInstance()
      cal.setTimeZone(TimeZone.getTimeZone("GMT"))
      cal.setTime(host)
      cal
    }

    @inline def toTimestamp = new java.sql.Timestamp(host.getTime)

    @inline def year: Int = toCalendar.get(Calendar.YEAR)

  }

}
