package com.qwery.runtime.datatypes

import com.qwery.implicits.MagicImplicits
import com.qwery.language.dieUnsupportedConversion
import com.qwery.language.models.ColumnType
import com.qwery.runtime.devices.FieldMetadata
import com.qwery.runtime.{INT_BYTES, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeBuffer
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.lang.BitArray

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

case class BitArrayType(maxSizeInBytes: Int, override val isExternal: Boolean = false)
  extends AbstractDataType(name = "BitArray") with FunctionalType[BitArray] {

  override def construct(args: Seq[Any]): BitArray = {
    args match {
      case Seq(array: Array[_]) => construct(array)
      case Seq(array: Seq[_]) => construct(array)
      case values =>
        BitArray(values.collect {
          case n: Number => n.longValue()
          case x => dieIllegalType(x)
        }: _*)
    }
  }

  override def convert(value: Any): BitArray = value match {
    case Some(v) => convert(v)
    case bitArray: BitArray => bitArray
    case buf: ByteBuffer => BitArray.decode(buf)
    case array: Array[_] => construct(array)
    case values: Seq[_] => construct(values)
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): BitArray = BitArray.decode(buf)

  override def encodeValue(value: Any): Array[Byte] = {
    convert(value).encode ~> { buf => allocate(INT_BYTES + buf.limit()).put(buf).flipMe().array() }
  }

  override def getJDBCType: Int = java.sql.Types.VARBINARY

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override def toColumnType: ColumnType = ColumnType(name = name, size = maxSizeInBytes)

  override def toJavaType(hasNulls: Boolean): Class[_] = classOf[BitArray]

  override def toSQL: String = s"$name($maxSizeInBytes)${if (isExternal) "*" else ""}"
}

/**
 * Represents a BitArray type with a default size (256-bytes)
 */
object BitArrayType extends BitArrayType(maxSizeInBytes = 256, isExternal = true) with ConstructorSupportCompanion with DataTypeParser {

  override def getCompatibleType(`class`: Class[_]): Option[DataType] = `class` match {
    case c if c == classOf[BitArray] => Some(BitArrayType)
    case _ => None
  }

  override def getCompatibleValue(value: Any): Option[DataType] = value match {
    case b: BitArray => Some(BitArrayType(b.size))
    case _ => None
  }

  override def parseDataType(columnType: ColumnType)(implicit scope: Scope): Option[DataType] = {
    if (synonyms contains columnType.name)
      Some((columnType.size.map(BitArrayType(_)) || BitArrayType).copy(isExternal = columnType.isPointer))
    else None
  }

  override def synonyms: Set[String] = Set("BitArray")

}

