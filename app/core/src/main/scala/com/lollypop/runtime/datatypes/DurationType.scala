package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.{ColumnType, Expression}
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.{INT_BYTES, LONG_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import com.lollypop.util.StringRenderHelper.StringRenderer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.concurrent.duration.{DurationLong, FiniteDuration}

/**
 * Represents a time-based duration (e.g. "7 days")
 */
class DurationType extends FixedLengthDataType(name = "Duration", maxSizeInBytes = LONG_BYTES + INT_BYTES)
  with FunctionalType[FiniteDuration] {

  override def construct(args: Seq[Any]): FiniteDuration = {
    args match {
      case Seq(arg0, arg1) =>
        (arg0, arg1) match {
          case (len: Number, unit: String) => FiniteDuration(len.longValue(), unit)
          case (_: Number, b) => die(s"Usage: Duration(length long, unit string) - '$b' is not a string")
          case (a, _: String) => die(s"Usage: Duration(length long, unit string) - '$a' is not a number")
          case (a, b) => die(s"Usage: Duration(length long, unit string) - '${a.renderAsJson}' is not a number and '${b.renderAsJson}' is not a string")
        }
      case Seq(value) => convert(value)
      case args => dieArgumentMismatch(args = args.size, minArgs = 1, maxArgs = 2)
    }
  }

  override def convert(value: Any): FiniteDuration = value match {
    case Some(v) => convert(v)
    case d: FiniteDuration => d
    case n: Number => n.longValue().millis
    case s: String =>
      val (length, unit) = s.split("[ ]") match {
        case Array(length) => (length.toDouble.toLong, "millis")
        case Array(length, unit) => (length.toDouble.toLong, unit.toLowerCase())
        case _ => dieExpectedInterval()
      }
      FiniteDuration(length, unit)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): FiniteDuration = buf.getInterval

  override def encodeValue(value: Any): Array[Byte] = allocate(maxSizeInBytes).putInterval(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.BIGINT

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[FiniteDuration]

  override def toSQL: String = name

}

object DurationType extends DurationType with ConstructorSupportCompanion with DataTypeParser {

  def apply(expression: Expression): NamedFunctionCall = {
    NamedFunctionCall(name = "Duration", List(expression))
  }

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[FiniteDuration] => Some(DurationType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: FiniteDuration => Some(DurationType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) => Some(DurationType)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Duration")

}