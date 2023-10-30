package com.lollypop.runtime.datatypes

import com.lollypop.runtime.INT_BYTES
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.CodecHelper.{deserialize, serialize}

import java.nio.ByteBuffer
import java.nio.ByteBuffer.allocate

/**
 * Represents a Serialized Data Type
 */
trait SerializedDataType[T] extends DataType with FunctionalType[T] {

  override def decode(buf: ByteBuffer): T = {
    val itemSize = buf.getInt
    val bytes = new Array[Byte](itemSize)
    buf.get(bytes)
    deserialize(bytes).asInstanceOf[T]
  }

  override def encodeValue(value: Any): Array[Byte] = value match {
    case item: Serializable =>
      val bytes = serialize(item)
      allocate(INT_BYTES + bytes.length).putInt(bytes.length).put(bytes).flipMe().array()
    case x => die(s"Serializable value expected '$x' (${x.getClass.getName})")
  }

  override def isExternal: Boolean = true

}
