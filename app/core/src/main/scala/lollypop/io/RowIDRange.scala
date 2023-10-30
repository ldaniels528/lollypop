package lollypop.io

import com.lollypop.die
import com.lollypop.runtime.{LONG_BYTES, ROWID}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.OptionHelper.OptionEnrichment

import java.nio.ByteBuffer

case class RowIDRange(start: Option[ROWID], end: Option[ROWID]) {

  def this(from: ROWID, to: ROWID) = this(start = Some(from), end = Some(to))

  def ++(range: RowIDRange): RowIDRange = RowIDRange(range.toList ::: this.toList)

  def apply(index: Int): ROWID = toList.apply(index)

  def encode: Array[Byte] = {
    val tuple = if (start == end) (start, None) else (start, end)
    val buf = tuple match {
      case (Some(a), Some(b)) =>
        val size = 2 * LONG_BYTES
        ByteBuffer.allocate(size + 1).put(size.toByte).putLong(a).putLong(b)
      case (Some(a), None) =>
        val size = LONG_BYTES
        ByteBuffer.allocate(size + 1).put(size.toByte).putLong(a)
      case (None, Some(b)) =>
        val size = LONG_BYTES
        ByteBuffer.allocate(size + 1).put(size.toByte).putLong(b)
      case (None, None) =>
        ByteBuffer.allocate(1).put(0.toByte)
    }
    buf.flipMe().array()
  }

  def headOption: Option[ROWID] = start ?? end

  def isEmpty: Boolean = headOption.isEmpty

  def toArray: Array[ROWID] = {
    (start, end) match {
      case (Some(a), Some(b)) => (a to b).toArray
      case (Some(a), None) => Array(a)
      case (None, Some(b)) => Array(b)
      case (None, None) => Array.empty
    }
  }

  def toList: List[ROWID] = {
    (start, end) match {
      case (Some(a), Some(b)) => (a to b).toList
      case (Some(a), None) => List(a)
      case (None, Some(b)) => List(b)
      case (None, None) => Nil
    }
  }

}

object RowIDRange extends Decoder[RowIDRange] {
  val maxSizeInBytes: Int = 2 * LONG_BYTES + 1

  def apply(rowIDs: List[ROWID]): RowIDRange = {
    if (rowIDs.isEmpty) RowIDRange(start = None, end = None)
    else if (rowIDs.size == 1) RowIDRange(start = rowIDs.headOption, end = None)
    else {
      val (a, b) = (rowIDs.min, rowIDs.max)
      if (a == b) RowIDRange(start = Some(a), end = None) else RowIDRange(start = Some(a), end = Some(b))
    }
  }

  def apply(rowIDs: ROWID*): RowIDRange = apply(rowIDs.toList)

  override def decode(buf: ByteBuffer): RowIDRange = {
    buf.get().toInt match {
      case 0 => RowIDRange(start = None, end = None)
      case n if n == LONG_BYTES => RowIDRange(buf.getLong)
      case n if n == 2 * LONG_BYTES => RowIDRange(start = Some(buf.getLong), end = Some(buf.getLong))
      case n => die(s"Illegal RowIDRange encoded length ($n)")
    }
  }
}
