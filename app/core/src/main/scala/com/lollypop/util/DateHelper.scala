package com.lollypop.util

import com.lollypop.runtime.JSON_DATE_FORMAT

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, TimeZone}

/**
 * Date Helper Utility
 */
object DateHelper {

  /**
   * Parses an ISO-8601 timestamp as a java.util.Date
   * @param isoDateString the date string (e.g. "2022-09-04T23:36:48.504Z")
   * @return the [[Date]]
   */
  def apply(isoDateString: String): Date = parse(isoDateString)

  /**
   * Parses an ISO-8601 timestamp as a java.sql.Timestamp
   * @param isoDateString the date string (e.g. "2022-09-04T23:36:48.504Z")
   * @return the [[java.sql.Timestamp]]
   */
  def ts(isoDateString: String): java.sql.Timestamp = new java.sql.Timestamp(parse(isoDateString).getTime)

  /**
   * Converts a date to an ISO-8601 formatted string (e.g. "2022-09-04T23:36:48.504Z")
   * @param date the date to convert
   * @return the [[Date]]
   */
  def formatAsTimestamp(date: Date): String = codec(_.format(date))

  /**
   * Converts a date to an ISO-8601 formatted string (e.g. "2022-09-04T23:36:48.504Z")
   * @param date the date to convert
   * @return the [[Date]]
   */
  def format(date: Date): String = date match {
    case d: java.sql.Date => d.toString
    case t: java.sql.Time => t.toString
    case t: java.sql.Timestamp => t.toString
    case d => codec(_.format(d))
  }

  def toHttpDate(date: Date): String = {
    val sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zz")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf.format(date)
  }

  def from(dateMillis: Long): Date = {
    val cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"))
    cal.setTimeInMillis(dateMillis)
    cal.getTime
  }

  def now: Date = from(System.currentTimeMillis())

  /**
   * Parses an ISO-8601 date
   * @param isoDateString the date string (e.g. "2022-09-04T23:36:48.504Z")
   * @return the [[Date]]
   */
  def parse(isoDateString: String): Date = codec(_.parse(isoDateString))

  /**
   * Parses an ISO-8601 date
   * @param dateString the date string (e.g. "2022-09-04T23:36:48.504Z")
   * @param dateFormat the date format (e.g. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
   * @return the [[Date]]
   */
  def parse(dateString: String, dateFormat: String): Date = codec(dateFormat)(_.parse(dateString))

  /**
   * Parses an ISO-8601 date
   * @param isoDateString the date string (e.g. "2022-09-04T23:36:48.504Z")
   * @return the [[Date]]
   */
  def parseTs(isoDateString: String): Date = {
    val sdf = new SimpleDateFormat(if (isoDateString.contains("T")) JSON_DATE_FORMAT else "yyyy-MM-dd HH:mm:ss.SSS")
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    sdf.parse(isoDateString)
  }

  private def codec[A](f: SimpleDateFormat => A): A = {
    val sdf = new SimpleDateFormat(JSON_DATE_FORMAT)
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    f(sdf)
  }

  private def codec[A](dateFormat: String)(f: SimpleDateFormat => A): A = {
    val sdf = new SimpleDateFormat(dateFormat)
    sdf.setTimeZone(TimeZone.getTimeZone("GMT"))
    f(sdf)
  }

}
