package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._
import com.lollypop.runtime.devices.FieldMetadata
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import lollypop.io.Encodable

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Character Large Object (CLOB) type
 * @example {{{
 *   CLOB('Hello World')
 * }}}
 */
object ClobType extends AbstractDataType(name = "CLOB") with FunctionalType[ICLOB]
  with ConstructorSupportCompanion with DataTypeParser {

  override def construct(args: Seq[Any]): ICLOB = {
    args match {
      case Nil => CLOB()
      case Seq(value) => convert(value)
      case s => dieArgumentMismatch(args = s.size, minArgs = 0, maxArgs = 1)
    }
  }

  @tailrec
  override def convert(value: Any): ICLOB = value match {
    case Some(v) => convert(v)
    case blob: java.sql.Blob => CLOB.fromInputStream(blob.getBinaryStream)
    case buf: ByteBuffer => CLOB.fromBytes(buf.array())
    case bytes: Array[Byte] => CLOB.fromBytes(bytes)
    case chars: Array[Char] => CLOB.fromChars(chars)
    case clob: ICLOB => clob
    case clob: java.sql.Clob => ICLOB(clob)
    case enc: Encodable => CLOB.fromBytes(enc.encode)
    case file: File => CLOB.fromFile(file)
    case s: String => CLOB.fromString(s)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): ICLOB = CLOB.fromString(buf.getText)

  override def encodeValue(value: Any): Array[Byte] = {
    val bytes = convert(value).getAsciiStream.readAllBytes()
    allocate(INT_BYTES + bytes.length).putBytes(bytes).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.CLOB

  override def isExternal: Boolean = true

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + maxSizeInBytes

  override def maxSizeInBytes: Int = PointerType.maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[java.sql.Clob]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c.isDescendantOf(classOf[java.sql.Clob]) => Some(ClobType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: java.sql.Clob => Some(ClobType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms.contains(columnType.name)) Some(ClobType) else None
  }

  override def synonyms: Set[String] = Set("CLOB")

}