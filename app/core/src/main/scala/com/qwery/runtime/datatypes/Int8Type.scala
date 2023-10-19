package com.qwery.runtime.datatypes

import com.qwery.language.dieUnsupportedConversion
import com.qwery.language.models.ColumnType
import com.qwery.runtime.{Boolean2Int, ONE_BYTE, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Tiny Integer (Byte) type
 */
object Int8Type extends FixedLengthNumericDataType(name = "Byte", maxSizeInBytes = ONE_BYTE)
  with FunctionalType[Byte] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Byte = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt.toByte
    case c: Char if c.isDigit => c.toByte
    case n: Number => n.byteValue()
    case s: String if s.contains("$") => convertCurrency(name, s).toByte
    case s: String => s.toByte
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Byte = buf.get

  override def encodeValue(value: Any): Array[Byte] = allocate(ONE_BYTE).put(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.TINYINT

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Byte] else classOf[Byte]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Byte] => Some(Int8Type)
    case c if c == classOf[java.lang.Byte] => Some(Int8Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Byte => Some(Int8Type)
    case _: java.lang.Byte => Some(Int8Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(Int8Type) else None
  }

  override def synonyms: Set[String] = Set("Byte")

}