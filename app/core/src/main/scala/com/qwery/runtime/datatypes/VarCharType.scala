package com.qwery.runtime.datatypes

import com.qwery.language.models.ColumnType
import com.qwery.runtime.devices.FieldMetadata
import com.qwery.runtime.{INT_BYTES, Scope}
import com.qwery.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import com.qwery.util.OptionHelper.OptionEnrichment

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

case class VarCharType(maxSizeInBytes: Int) extends AbstractDataType(name = "VarChar") with FunctionalType[Array[Char]] {

  override def convert(value: Any): Array[Char] = value match {
    case Some(v) => convert(v)
    case c: Array[Char] => c
    case x => StringType.convert(x).toCharArray
  }

  override def decode(buf: ByteBuffer): Array[Char] = {
    val bytes = new Array[Byte](buf.getLength32)
    buf.get(bytes)
    new String(bytes).toCharArray
  }

  override def encodeValue(value: Any): Array[Byte] = {
    val bytes = String.valueOf(convert(value)).getBytes()
    allocate(maxSizeInBytes).putInt(bytes.length).put(bytes).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.VARCHAR

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[Array[Char]]

  override def toSQL: String = s"$name($maxSizeInBytes)"

}

/**
 * Represents a Character String type
 */
object VarCharType extends VarCharType(maxSizeInBytes = 8192) with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[Array[Char]] => Some(VarCharType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case chars: Array[Char] => Some(VarCharType(maxSizeInBytes = chars.length))
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms.contains(columnType.name)) Some(columnType.size.map(VarCharType(_)) || VarCharType) else None
  }

  override def synonyms: Set[String] = Set("VarChar")

}
