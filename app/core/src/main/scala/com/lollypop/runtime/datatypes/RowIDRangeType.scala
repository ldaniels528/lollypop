package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.Scope
import lollypop.io.RowIDRange

import java.nio.ByteBuffer

/**
 * Represents a Row ID range type
 */
abstract class RowIDRangeType extends FixedLengthDataType(name = "RowIDRange", maxSizeInBytes = RowIDRange.maxSizeInBytes)
  with FunctionalType[RowIDRange] {

  override def convert(value: Any): RowIDRange = value match {
    case Some(v) => convert(v)
    case r: RowIDRange => r
    case r: Range => RowIDRange(rowIDs = r.toList.map(_.toLong))
    case a: Array[_] if a.forall(_.isInstanceOf[Number]) => RowIDRange(a.collect { case n: Number => n.longValue() }: _*)
    case s: Seq[_] if s.forall(_.isInstanceOf[Number]) => RowIDRange(s.collect { case n: Number => n.longValue() }: _*)
    case s: String if s.startsWith("[") && s.endsWith("]") =>
      val span = s.drop(1).dropRight(1).trim
      if (span.nonEmpty) RowIDRange(rowIDs = span.split("[,]").map(_.trim.toLong): _*) else RowIDRange()
    case x => dieUnsupportedConversion(x, name)
  }

  override def construct(args: Seq[Any]): RowIDRange = convert(args)

  override def decode(buf: ByteBuffer): RowIDRange = RowIDRange.decode(buf)

  override def encodeValue(value: Any): Array[Byte] = convert(value).encode

  override def getJDBCType: Int = java.sql.Types.ARRAY

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[RowIDRange]

  override def toSQL: String = name

}

object RowIDRangeType extends RowIDRangeType with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[RowIDRange] => Some(RowIDRangeType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: RowIDRange => Some(RowIDRangeType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case "RowIDRange" => Some(RowIDRangeType)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("RowIDRange")

  override def toSQL: String = name

}