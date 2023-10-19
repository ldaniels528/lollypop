package com.qwery.runtime.datatypes

import com.qwery.language.dieUnsupportedConversion
import com.qwery.language.models.ColumnType
import com.qwery.runtime.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.devices.FieldMetadata
import com.qwery.runtime.{INT_BYTES, Scope}
import com.qwery.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import qwery.io.Encodable

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a SQL XML type
 * @example {{{
 *   SQLXML('Hello World')
 * }}}
 */
object SQLXMLType extends AbstractDataType(name = "SQLXML") with DataTypeParser with FunctionalType[ISQLXML] {

  override def construct(args: Seq[Any]): ISQLXML = {
    args match {
      case Nil => SQLXML()
      case Seq(value) => convert(value)
      case s => dieArgumentMismatch(args = s.size, minArgs = 0, maxArgs = 1)
    }
  }

  @tailrec
  override def convert(value: Any): ISQLXML = value match {
    case Some(v) => convert(v)
    case blob: java.sql.Blob => SQLXML.fromInputStream(blob.getBinaryStream)
    case buf: ByteBuffer => SQLXML.fromBytes(buf.array())
    case bytes: Array[Byte] => SQLXML.fromBytes(bytes)
    case chars: Array[Char] => SQLXML.fromChars(chars)
    case clob: java.sql.Clob => SQLXML.fromInputStream(clob.getAsciiStream)
    case enc: Encodable => convert(enc.encode)
    case file: File => SQLXML.fromFile(file)
    case s: String => SQLXML.fromString(s)
    case sqlXML: ISQLXML => sqlXML
    case sqlXML: java.sql.SQLXML => ISQLXML(sqlXML)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): ISQLXML = SQLXML.fromBytes(buf.getBytes)

  override def encodeValue(value: Any): Array[Byte] = {
    val sqlXML = convert(value)
    val bytes = sqlXML.getBinaryStream.readAllBytes()
    allocate(INT_BYTES + bytes.length).putBytes(bytes).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.SQLXML

  override def isExternal: Boolean = true

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + maxSizeInBytes

  override def maxSizeInBytes: Int = PointerType.maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[ISQLXML]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c.isDescendantOf(classOf[java.sql.Clob]) => Some(this)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: ISQLXML => Some(this)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    columnType.name match {
      case "SQLXML" => Some(this)
      case "XML" => Some(this)
      case _ => None
    }
  }

  override def synonyms: Set[String] = Set("SQLXML", "XML")

}
