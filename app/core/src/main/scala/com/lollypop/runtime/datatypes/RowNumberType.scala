package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._
import lollypop.io.Decoder

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Sequence Number type
 */
object RowNumberType extends FixedLengthNumericDataType(name = "RowNumber", maxSizeInBytes = LONG_BYTES)
  with Decoder[Long] with DataTypeParser {

  @tailrec
  override def convert(value: Any): Long = value match {
    case Some(v) => convert(v)
    case n: Number => n.longValue()
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Long = buf.getLong

  override def encodeValue(value: Any): Array[Byte] = allocate(LONG_BYTES).putLong(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.ROWID

  override def isAutoIncrement: Boolean = true

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Long] else classOf[Long]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = None

  override def getCompatibleValue(value: Any): Option[DataType] = None

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(RowNumberType) else None
  }

  override def synonyms: Set[String] = Set(name)

  override def toSQL: String = name

}