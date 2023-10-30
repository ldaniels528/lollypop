package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.{SHORT_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a Character type
 */
abstract class CharType extends FixedLengthDataType(name = "Char", maxSizeInBytes = SHORT_BYTES)
  with FunctionalType[Char] {

  override def convert(value: Any): Char = value match {
    case Some(v) => convert(v)
    case c: Char => c
    case n: Number => n.intValue().toChar
    case s: String => s.headOption.getOrElse(0.toChar)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Char = buf.getChar

  override def encodeValue(value: Any): Array[Byte] = allocate(SHORT_BYTES).putChar(convert(value)).flipMe().array()

  override def getJDBCType: Int = java.sql.Types.CHAR

  override def toJavaType(hasNulls: Boolean): Class[_] = if (hasNulls) classOf[Character] else classOf[Char]

}

case object CharType extends CharType with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Char] => Some(CharType)
    case c if c == classOf[java.lang.Character] => Some(CharType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: Char => Some(CharType)
    case _: java.lang.Character => Some(CharType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case s if synonyms.contains(s) => Some(columnType.size.collect { case n if n > 1 => StringType(n) } getOrElse CharType)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("Char")

}
