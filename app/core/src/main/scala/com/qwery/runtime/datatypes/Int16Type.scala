package com.qwery.runtime.datatypes

import com.qwery.language.dieUnsupportedConversion
import com.qwery.language.models.ColumnType
import com.qwery.runtime.{Boolean2Int, SHORT_BYTES, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Small Integer type
 */
object Int16Type extends FixedLengthNumericDataType(name = "Short", maxSizeInBytes = SHORT_BYTES)
  with FunctionalType[Short] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Short = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt.toShort
    case c: Char if c.isDigit => c.toShort
    case n: Number => n.shortValue()
    case s: String if s.contains("$") => convertCurrency(name, s).toShort
    case s: String => s.toShort
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Short = buf.getShort

  override def encodeValue(value: Any): Array[Byte] = allocate(SHORT_BYTES).putShort(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.SMALLINT

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Short] else classOf[Short]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Short] => Some(Int16Type)
    case c if c == classOf[java.lang.Short] => Some(Int16Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Short => Some(Int16Type)
    case _: java.lang.Short => Some(Int16Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(Int16Type) else None
  }

  override def synonyms: Set[String] = Set("Short")

}