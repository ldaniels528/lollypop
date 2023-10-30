package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.lollypop.runtime.devices.{FieldMetadata, RowCollection}
import com.lollypop.runtime.{INT_BYTES, Scope}
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import lollypop.io.Encodable

import java.io.File
import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import scala.annotation.tailrec

/**
 * Represents a Binary Large Object (BLOB) type
 * @example {{{
 *   BLOB('Hello World')
 * }}}
 */
object BlobType extends AbstractDataType(name = "BLOB") with FunctionalType[IBLOB]
  with ConstructorSupportCompanion
  with DataTypeParser {

  override def construct(args: Seq[Any]): IBLOB = {
    args match {
      case Nil => BLOB()
      case Seq(value) => convert(value)
      case s => dieArgumentMismatch(args = s.size, minArgs = 0, maxArgs = 1)
    }
  }

  @tailrec
  override def convert(value: Any): IBLOB = value match {
    case Some(v) => convert(v)
    case blob: IBLOB => blob
    case blob: java.sql.Blob => IBLOB(blob)
    case buf: ByteBuffer => BLOB.fromBytes(buf.array())
    case bytes: Array[Byte] => BLOB.fromBytes(bytes)
    case clob: java.sql.Clob => BLOB.fromInputStream(clob.getAsciiStream)
    case file: File => BLOB.fromFile(file)
    case rc: RowCollection => BLOB.fromRowCollection(rc)
    case s: String => BLOB.fromString(s)
    case enc: Encodable => BLOB.fromBytes(enc.encode)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): IBLOB = BLOB.fromBytes(buf.getBytes)

  override def encodeValue(value: Any): Array[Byte] = {
    val bytes = convert(value).getBinaryStream.readAllBytes()
    allocate(INT_BYTES + bytes.length).putBytes(bytes).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.BLOB

  override def isExternal: Boolean = true

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + maxSizeInBytes

  override def maxSizeInBytes: Int = PointerType.maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[java.sql.Blob]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c.isDescendantOf(classOf[java.sql.Blob]) => Some(BlobType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: java.sql.Blob => Some(BlobType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(BlobType) else None
  }

  override def synonyms: Set[String] = Set("BLOB")

}