package lollypop.lang

import com.lollypop.language.dieIllegalObjectRef
import com.lollypop.language.models.AllFields.dieArgumentMismatch
import com.lollypop.language.models.Instruction
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{DataType, PointerType}
import lollypop.io.Encodable

import java.nio.ByteBuffer

/**
 * Represents a reference (pointer) to data residing in BLOB storage
 * @param offset    the offset of the referenced data in the file
 * @param allocated the allocated size for storing the referenced data
 * @param length    the actual size of the referenced data
 */
case class Pointer(offset: Long, allocated: Long, length: Long) extends Encodable with Instruction with LollypopNative {

  override def encode: Array[Byte] = {
    ByteBuffer.allocate(Pointer.SIZE_IN_BYTES)
      .putLong(offset)
      .putLong(allocated)
      .putLong(length)
      .flipMe()
      .array()
  }

  override def returnType: DataType = PointerType

  override def toSQL: String = s"Pointer($offset, $allocated, $length)"

  override def toString: String = toSQL

}

/**
 * Pointer Reference Companion
 */
object Pointer {
  val SIZE_IN_BYTES: Int = 3 * LONG_BYTES

  def decode(buf: ByteBuffer): Pointer = {
    Pointer(offset = buf.getLong, allocated = buf.getLong, length = buf.getLong)
  }

  def parse(ref: String): Pointer = {
    assert(ref.toLowerCase().startsWith("pointer(") && ref.endsWith(")"), dieIllegalObjectRef(ref))
    val args = ref.drop(8).dropRight(1)
    args.split("[,]").map(_.trim) match {
      case Array(offset, allocated, length) =>
        try
          Pointer(offset = offset.toLong, allocated = allocated.toLong, length = length.toLong)
        catch {
          case _: Exception => dieIllegalObjectRef(ref)
        }
      case args => dieArgumentMismatch(args = args.length, minArgs = 3, maxArgs = 3)
    }
  }

}