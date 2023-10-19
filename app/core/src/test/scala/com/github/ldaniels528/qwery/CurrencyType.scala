package com.github.ldaniels528.qwery

import com.qwery.language.dieUnsupportedConversion
import com.qwery.runtime.LONG_BYTES
import com.qwery.runtime.datatypes.{ConstructorSupportCompanion, FixedLengthNumericDataType, FunctionalType}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a Currency type
 */
class CurrencyType extends FixedLengthNumericDataType(name = "CurrencyType", maxSizeInBytes = LONG_BYTES)
  with FunctionalType[Double] with ConstructorSupportCompanion {

  override def convert(value: Any): Double = value match {
    case Some(v) => convert(v)
    case n: Number => n.doubleValue()
    case s: String =>
      val text = s.toUpperCase.filterNot(c => c == '$' || c == ',') // e.g., $54M or $11,000.00
      text match {
        case v if v.endsWith("K") => v.dropRight(1).toDouble * 1e+3
        case v if v.endsWith("M") => v.dropRight(1).toDouble * 1e+6
        case v if v.endsWith("B") => v.dropRight(1).toDouble * 1e+9
        case v if v.endsWith("T") => v.dropRight(1).toDouble * 1e+12
        case v if v.endsWith("Q") => v.dropRight(1).toDouble * 1e+15
        case v => v.toDouble
      }
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Double = buf.getDouble

  override def encodeValue(value: Any): Array[Byte] = allocate(maxSizeInBytes).putDouble(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.DECIMAL

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Double] else classOf[Double]

}
