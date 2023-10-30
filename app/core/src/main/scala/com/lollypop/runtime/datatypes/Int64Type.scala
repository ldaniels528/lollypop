package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.{Boolean2Int, LONG_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import java.util.Date
import scala.annotation.tailrec
import scala.concurrent.duration.Duration

/**
 * Represents a Long Integer type
 */
object Int64Type extends FixedLengthNumericDataType(name = "Long", maxSizeInBytes = LONG_BYTES)
  with FunctionalType[Long] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Long = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt
    case c: Char if c.isDigit => c.toLong
    case d: Date => d.getTime
    case d: Duration => d.toMillis
    case n: Number => n.longValue()
    case s: String if s.contains("$") => convertCurrency(name, s).toLong
    case s: String => s.toLong
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Long = buf.getLong

  override def encodeValue(value: Any): Array[Byte] = allocate(LONG_BYTES).putLong(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.BIGINT

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Long] else classOf[Long]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Long] => Some(Int64Type)
    case c if c == classOf[java.lang.Long] => Some(Int64Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Long => Some(Int64Type)
    case _: java.lang.Long => Some(Int64Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(Int64Type) else None
  }

  override def synonyms: Set[String] = Set("Long")

}