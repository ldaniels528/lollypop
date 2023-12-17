package com.lollypop.runtime.datatypes

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_IMPERATIVE}
import com.lollypop.language.models.ColumnType
import com.lollypop.language.{ColumnTypeParser, HelpDoc, SQLCompiler, TokenStream, dieUnsupportedConversion}
import com.lollypop.runtime._
import com.lollypop.util.DateHelper

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId}
import java.util.Date

/**
 * Represents a Date/Time type
 * @param format the date format
 * @example {{{
 *   val t = DateTime('2023-07-17T22:10:10.829Z')
 * }}}
 */
case class DateTimeType(format: String) extends FixedLengthDataType(name = "DateTime", maxSizeInBytes = LONG_BYTES)
  with FunctionalType[Date] {

  override def construct(args: Seq[Any]): Date = args match {
    case Nil => DateHelper.now
    case Seq(value) => convert(value)
    case x => convert(x)
  }

  override def convert(value: Any): Date = value match {
    case Some(v) => convert(v)
    case d: Date => d
    case d: LocalDate => Date.from(d.atTime(LocalTime.MIDNIGHT).atZone(ZoneId.systemDefault()).toInstant)
    case d: LocalDateTime => Date.from(d.atZone(ZoneId.systemDefault()).toInstant)
    case n: Number => DateHelper.from(n.longValue())
    case s: String if s.matches("\\d+") => convert(s.toLong)
    case s: String =>
      try DateHelper.parse(s, format) catch {
        case e0: Exception =>
          try DateHelper.parseTs(s) catch {
            case _: Exception => die(e0.getMessage, e0)
          }
      }
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Date = buf.getDate

  override def encodeValue(value: Any): Array[Byte] = allocate(LONG_BYTES).putDate(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.TIMESTAMP

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[java.util.Date]

  override def toSQL: String = if (format == ISO_8601_DATE_FORMAT) name else s"$name(\"$format\")"

}

object DateTimeType extends DateTimeType(ISO_8601_DATE_FORMAT) with ConstructorSupportCompanion
  with ColumnTypeParser with DataTypeParser {
  private val classes = Seq(
    classOf[java.util.Date], classOf[java.sql.Date], classOf[LocalDate], classOf[LocalDateTime],
    classOf[java.sql.Time], classOf[java.sql.Timestamp]
  )

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = {
    if (classes.contains(`class`)) Option(DateTimeType) else None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = {
    val `class` = Option(value).map(_.getClass).orNull
    if (classes.contains(`class`)) Option(DateTimeType) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = name,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"DateTime()",
    description = "Creates new date instance",
    example = s"DateTime()"))

  override def parseColumnType(stream: TokenStream)(implicit compiler: SQLCompiler): Option[ColumnType] = {
    if (understands(stream)) {
      val typeName = stream.next().valueAsString
      Option(ColumnType(typeName).copy(typeArgs =
        stream.captureIf("(", ")", delimiter = None) {
          _.next().valueAsString
        }))
    } else None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) & columnType.typeArgs.nonEmpty =>
        columnType.typeArgs match {
          case Nil => Some(DateTimeType)
          case Seq(format) => Some(DateTimeType(format))
          case args => dieArgumentMismatch(args.length, minArgs = 1, maxArgs = 1)
        }
      case s if synonyms.contains(s) => Some(DateTimeType)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("DateTime")

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = synonyms.exists(ts is _)

}