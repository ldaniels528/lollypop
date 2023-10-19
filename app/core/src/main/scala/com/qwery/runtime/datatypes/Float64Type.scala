package com.qwery.runtime.datatypes

import com.qwery.language.dieUnsupportedConversion
import com.qwery.language.models.ColumnType
import com.qwery.runtime.{Boolean2Int, LONG_BYTES, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec
import scala.concurrent.duration.Duration

/**
 * Represents a Double type
 */
case object Float64Type extends FixedLengthNumericDataType(name = "Double", maxSizeInBytes = LONG_BYTES)
  with FunctionalType[Double] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Double = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt
    case c: Char => c.toDouble
    case d: Duration => d.toMillis.toDouble
    case d: java.util.Date => d.getTime.toDouble
    case n: Number => n.doubleValue()
    case s: String if s.contains("$") => convertCurrency(name, s)
    case s: String => s.toDouble
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Double = buf.getDouble

  override def encodeValue(value: Any): Array[Byte] = allocate(LONG_BYTES).putDouble(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.DOUBLE

  override def isFloatingPoint: Boolean = true

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Double] else classOf[Double]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Double] => Some(Float64Type)
    case c if c == classOf[java.lang.Double] => Some(Float64Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Double => Some(Float64Type)
    case _: java.lang.Double => Some(Float64Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) => Some(Float64Type)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Double")

}