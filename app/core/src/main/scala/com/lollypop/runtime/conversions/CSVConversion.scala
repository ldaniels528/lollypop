package com.lollypop.runtime.conversions

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.conditions.BooleanLiteral
import com.lollypop.runtime.{DECIMAL_NUMBER_REGEX, INTEGER_NUMBER_REGEX, ISO_8601_REGEX, UUID_REGEX}
import com.lollypop.util.DateHelper
import lollypop.lang.Null

import java.util.UUID
import scala.annotation.tailrec

/**
 * CSV Conversion
 */
object CSVConversion extends Conversion {

  @tailrec
  override def convert(value: Any): Option[Any] = value match {
    case null | None => None
    case Some(v) => convert(v)
    case "false" => Some(false)
    case "true" => Some(true)
    case s: String if s.isEmpty => None
    case s: String if s.matches(ISO_8601_REGEX) => Some(DateHelper(s))
    case s: String if s.matches(UUID_REGEX) => Some(UUID.fromString(s))
    case s: String if s.matches(INTEGER_NUMBER_REGEX) => Some(s.toLong)
    case s: String if s.matches(DECIMAL_NUMBER_REGEX) => Some(s.toDouble)
    case x => Some(String.valueOf(x))
  }

  def convertToExpression(value: String): Option[Expression] = value match {
    case null => Some(Null())
    case "false" => Some(BooleanLiteral(false))
    case "true" => Some(BooleanLiteral(true))
    case s: String if s.isEmpty => Some(Null())
    case s: String if s.matches(ISO_8601_REGEX) => Some(DateHelper(s).v)
    case s: String if s.matches(UUID_REGEX) => Some(UUID.fromString(s).v)
    case s: String if s.matches(INTEGER_NUMBER_REGEX) => Some(s.toLong.v)
    case s: String if s.matches(DECIMAL_NUMBER_REGEX) => Some(s.toDouble.v)
    case _ => None
  }

}
