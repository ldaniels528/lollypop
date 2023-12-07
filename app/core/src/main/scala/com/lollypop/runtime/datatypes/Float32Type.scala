package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Float type
 */
object Float32Type extends FixedLengthNumericDataType(name = "Float", maxSizeInBytes = INT_BYTES)
  with FunctionalType[Float] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): Float = value match {
    case Some(v) => convert(v)
    case b: Boolean => b.toInt
    case c: Char => c.toFloat
    case n: Number => n.floatValue()
    case s: String if s.contains("$") => convertCurrency(name, s).toFloat
    case s: String => s.toFloat
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Float = buf.getFloat

  override def encodeValue(value: Any): Array[Byte] = allocate(INT_BYTES).putFloat(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.FLOAT

  override def isFloatingPoint: Boolean = true

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[java.lang.Float] else classOf[Float]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Float] => Some(Float32Type)
    case c if c == classOf[java.lang.Float] => Some(Float32Type)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Float => Some(Float32Type)
    case _: java.lang.Float => Some(Float32Type)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(Float32Type) else None
  }

  override def synonyms: Set[String] = Set("Float")

}