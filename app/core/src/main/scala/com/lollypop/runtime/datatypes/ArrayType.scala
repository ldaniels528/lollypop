package com.lollypop.runtime.datatypes

import com.lollypop.language.dieUnsupportedConversion
import com.lollypop.language.models.ColumnType
import com.lollypop.runtime.INT_BYTES
import com.lollypop.runtime.plastics.Tuples.seqToArray
import com.lollypop.runtime.devices.FieldMetadata
import com.lollypop.util.ByteBufferHelper.{DataTypeBuffer, DataTypeByteBuffer}
import com.lollypop.util.OptionHelper.OptionEnrichment

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents an Array type
 * @param componentType the [[DataType type]] of the array entries
 * @param capacity      the optional maximum capacity for array entries
 */
case class ArrayType(componentType: DataType, capacity: Option[Int] = None) extends AbstractDataType(name = "ARRAY")
  with FunctionalType[Array[_]] {

  override def construct(args: Seq[Any]): Array[_] = convert(args)

  override def convert(value: Any): Array[_ <: Any] = value match {
    case Some(v) => convert(v)
    case array if array.getClass.isArray => array.asInstanceOf[Array[_ <: Any]]
    case seq: Seq[Any] => seq.toArray
    case s: String => s.toCharArray
    case x => dieUnsupportedConversion(x, name)
  }

  override def decode(buf: ByteBuffer): Array[_] = {
    val itemCount = buf.getLength32
    seqToArray(values = (0 until itemCount) map { _ => componentType.decodeFull(buf)._2.orNull })
  }

  override def encodeValue(value: Any): Array[Byte] = value match {
    case array: Array[_] =>
      val bytes = array.flatMap(v => componentType.encodeFull(v).array())
      val itemCount = array.length
      allocate(INT_BYTES + bytes.length).putInt(itemCount).put(bytes).flipMe().array()
    case x => die(s"Unsupported array value '$x' (${x.getClass.getName})")
  }

  override def getJDBCType: Int = java.sql.Types.ARRAY

  override def isExternal: Boolean = capacity.isEmpty

  override def maxPhysicalSize: Int = FieldMetadata.BYTES_LENGTH + INT_BYTES + maxSizeInBytes

  override val maxSizeInBytes: Int = capacity.map(_ * componentType.maxPhysicalSize) || PointerType.maxSizeInBytes

  override def toColumnType: ColumnType = componentType.toColumnType.copy(arrayArgs = capacity.toList.map(_.toString))

  override def toJavaType(hasNulls: Boolean): Class[_] = {
    java.lang.reflect.Array.newInstance(componentType.toJavaType(hasNulls), 0).getClass
  }

  override def toSQL: String = s"${componentType.toSQL}[${capacity.getOrElse("")}]"

}

/**
 * Represents an Array type
 */
object ArrayType extends ArrayType(componentType = AnyType, capacity = None)
