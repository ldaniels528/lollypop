package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.{Boolean2Int, INT_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec
import scala.concurrent.duration.Duration

/**
 * Represents an Integer type
 */
case object Int32Type extends FixedLengthNumericDataType(name = "Int", maxSizeInBytes = INT_BYTES)
  with FunctionalType[Int] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Int = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt
    case c: Char => c.toInt
    case d: Duration => d.toMillis.toInt
    case n: Number => n.intValue()
    case s: String if s.contains("$") => convertCurrency(name, s).toInt
    case s: String => s.toInt
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Int = buf.getInt

  override def encodeValue(value: Any): Array[Byte] = allocate(INT_BYTES).putInt(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.INTEGER

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Integer] else classOf[Int]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Int] => Some(Int32Type)
    case c if c == classOf[java.lang.Integer] => Some(Int32Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Int => Some(Int32Type)
    case _: java.lang.Integer => Some(Int32Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(Int32Type) else None
  }

  override def synonyms: Set[String] = Set("Int", "Integer")

  override def toSQL: String = name

}