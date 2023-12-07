package com.lollypop.runtime.datatypes

import com.lollypop.language.models.ColumnType
import com.lollypop.runtime._

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate
import java.util.UUID
import scala.annotation.tailrec

/**
 * Represents an UUID type
 */
object UUIDType extends FixedLengthDataType(name = "UUID", maxSizeInBytes = 2 * LONG_BYTES)
  with FunctionalType[UUID] with ConstructorSupportCompanion with DataTypeParser {

  @tailrec
  override def convert(value: Any): UUID = value match {
    case Some(v) => convert(v)
    case s: String => UUID.fromString(s)
    case u: UUID => u
    case _ => null //dieUnsupportedConversion(x, name)
  }

  override def construct(args: Seq[Any]): UUID = {
    args.length match {
      case 0 => UUID.randomUUID()
      case 1 => convert(args.head)
      case n => dieArgumentMismatch(n, minArgs = 0, maxArgs = 0)
    }
  }

  override def decode(buf: ByteBuffer): UUID = buf.getUUID

  override def encodeValue(value: Any): Array[Byte] = {
    allocate(2 * LONG_BYTES).putUUID(convert(value)).flipMe().array()
  }

  override def getJDBCType: Int = java.sql.Types.BINARY

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[UUID]

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[UUID] => Some(UUIDType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case _: UUID => Some(UUIDType)
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name) Some(UUIDType) else None
  }

  override def synonyms: Set[String] = Set("UUID")

}