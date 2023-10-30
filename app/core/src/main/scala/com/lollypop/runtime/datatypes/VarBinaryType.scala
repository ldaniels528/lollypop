package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.devices.FieldMetadata
import com.lollypop.runtime.{INT_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.CodecHelper
import com.lollypop.util.OptionHelper.OptionEnrichment

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

case class VarBinaryType(maxSizeInBytes: Int) extends AbstractDataType(name = "VarBinary") with FunctionalType[Array[Byte]] {

  override def convert(value: Any): Array[Byte] = value match {
    case Some(v) => convert(v)
    case blob: java.sql.Blob => blob.getBinaryStream.readAllBytes()
    case byteBuffer: ByteBuffer => byteBuffer.array()
    case bytes: Array[Byte] => bytes
    case chars: Array[Char] => convert(String.valueOf(chars))
    case clob: java.sql.Clob => clob.getAsciiStream.readAllBytes()
    case sqlXml: java.sql.SQLXML => sqlXml.getBinaryStream.readAllBytes()
    case string: String => string.getBytes()
    case value: Serializable => CodecHelper.serialize(value)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Array[Byte] = {
    val size = buf.getInt
    assert(size >= 0, die("Negative array size detected"))
    val bytes = new Array[Byte](size)
    buf.get(bytes)
    bytes
  }

  override def encodeValue(value: Any): Array[Byte] = {
    val bytes = convert(value)
    allocate(maxSizeInBytes).putInt(bytes.length).put(bytes).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.VARBINARY

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[Array[Byte]]

  override def toSQL: String = s"$name($maxSizeInBytes)"

}

/**
 * Represents a Binary String type
 */
object VarBinaryType extends VarBinaryType(maxSizeInBytes = 8192) with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Array[Byte]] => Some(VarBinaryType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case b: Array[Byte] => Some(VarBinaryType(maxSizeInBytes = b.length))
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms.contains(columnType.name)) Some(columnType.size.map(VarBinaryType(_)) || VarBinaryType) else None
  }

  override def synonyms: Set[String] = Set("VarBinary")

}

