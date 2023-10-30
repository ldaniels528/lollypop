package com.lollypop.util

import scala.collection.mutable
import scala.language.reflectiveCalls

/**
 * String Helper
 * @author lawrence.daniels@gmail.com
 */
object StringHelper {

  private type StringLike = {
    def indexOf(s: String): Int
    def indexOf(s: String, start: Int): Int
    def lastIndexOf(s: String): Int
    def lastIndexOf(s: String, start: Int): Int
  }

  @inline
  private def toOption(index: Int): Option[Int] = index match {
    case -1 => None
    case _ => Some(index)
  }

  /**
   * StringLike Index Enrichment
   * @param string the given [[StringLike]]; e.g. a [[String]] or [[StringBuilder]]
   */
  final implicit class StringLikeIndexEnrichment[T <: StringLike](val string: T) extends AnyVal {

    @inline def indexOfOpt(s: String): Option[Int] = toOption(string.indexOf(s))

    @inline def indexOfOpt(s: String, start: Int): Option[Int] = toOption(string.indexOf(s, start))

    @inline def lastIndexOfOpt(s: String): Option[Int] = toOption(string.lastIndexOf(s))

    @inline def lastIndexOfOpt(s: String, limit: Int): Option[Int] = toOption(string.lastIndexOf(s, limit))

  }

  /**
   * String Enrichment
   * @param string the given [[String]]
   */
  final implicit class StringEnrichment(val string: String) extends AnyVal {

    @inline
    def delimitedSplit(delimiter: Char, withQuotes: Boolean = false): List[String] = {
      var inQuotes = false
      var leftOver = false
      val sb = new mutable.StringBuilder()
      val values = string.toCharArray.foldLeft[List[String]](Nil) {
        case (list, ch) if ch == '"' =>
          inQuotes = !inQuotes
          if (withQuotes) sb.append(ch)
          list
        case (list, ch) if inQuotes & ch == delimiter =>
          sb.append(ch); list
        case (list, ch) if !inQuotes & ch == delimiter =>
          val s = sb.toString()
          sb.clear()
          leftOver = true
          list ::: s.trim :: Nil
        case (list, ch) =>
          leftOver = false
          sb.append(ch)
          list
      }
      if (sb.toString().nonEmpty || leftOver) values ::: sb.toString().trim :: Nil else values
    }

    @inline
    def isQuoted: Boolean = {
      (for {
        a <- string.headOption
        b <- string.lastOption
      } yield a == b && (a == '"' || a == '\'') || a == '`') contains true
    }

    @inline
    def limit(length: Int, ellipses: String = " ... "): String = {
      if (string.length < length) string else string.take(length) + ellipses
    }

    @inline def noneIfBlank: Option[String] = string.trim match {
      case s if s.isEmpty => None
      case s => Option(s)
    }

    @inline def singleLine: String = string.split("\n").map(_.trim).mkString(" ")

    @inline def toCamelCase: String = string match {
      case s if s.length > 1 => s.head.toLower + s.tail
      case s => s.toLowerCase
    }
  }

}
