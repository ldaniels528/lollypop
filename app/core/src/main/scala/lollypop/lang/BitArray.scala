package lollypop.lang

import com.lollypop.implicits.MagicImplicits
import com.lollypop.runtime.datatypes.{BitArrayType, Int64Type, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.runtime.{Boolean2Int, INT_BYTES, LONG_BYTES, LollypopNative, Scope}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.Decoder
import lollypop.lang.BitArray.{BitArrayExtensions, ChangeListener, bitsPerRow, computeArraySize, encodedSize, setMask, toBinary, unsetMask}

import java.nio.ByteBuffer

/**
 * Creates a new bit array declaring the internal array size and the minimum value.
 * @param initialSize the internal array size, which determines the largest value that can be
 *                    represented within the bit array
 * @param floor       the lower bound of the acceptable value range; defaults to zero
 * @return a new [[BitArray bit array]]
 */
class BitArray(initialSize: Int, var floor: Long = 0) extends LollypopNative with TableRendering {
  private var array = new Array[Long](initialSize)
  private var changeListeners: List[ChangeListener] = Nil

  def ++(bitArray: BitArray): BitArray = {
    BitArray(ascending ::: bitArray.ascending: _*)
  }

  def add(values: Long*): Unit = {
    val changed = values.foldLeft(false) { (changed, value) =>
      val (row, col) = ensureCoordinates(value)
      val before = array(row)
      array(row) |= setMask(col)
      changed || (before != array(row))
    }
    if (changed) notifyChangeListeners()
  }

  def addChangeListener(listener: ChangeListener): Unit = {
    changeListeners = listener :: changeListeners
  }

  /**
   * Returns the value of the bit at an index within is set
   * @param index the bit's index
   * @return 0 or 1
   */
  def apply(index: Int): Int = isSet(index).toInt

  def ascending: List[Long] = {
    for {
      row <- array.indices.toList
      bits = array(row) if bits != 0
      col <- 0 until bitsPerRow
      value = toValue(row, col) if bits.isOn(col)
    } yield value
  }

  def capacity: Int = array.length

  def contains(value: Long): Boolean = {
    val (row, col) = toCoordinates(value)
    (row < array.length) && array(row).isOn(col)
  }

  def count(f: Long => Boolean): Int = {
    foldLeft(0) { (total, value) => if (f(value)) total + 1 else total }
  }

  def descending: List[Long] = {
    for {
      row <- array.indices.reverse.toList
      bits = array(row) if bits != 0
      col <- (0 until bitsPerRow).reverse
      value = toValue(row, col) if bits.isOn(col)
    } yield value
  }

  def dump: Seq[String] = {
    for {bits <- array if bits != 0} yield toBinary(bits).replace('0', '.')
  }

  def encode: ByteBuffer = {
    val buf = ByteBuffer.allocate(encodedSize(array.length))
    buf.putLong(floor)
    buf.putInt(array.length)
    array.foreach(buf.putLong)
    buf.flipMe()
  }

  override def equals(obj: Any): Boolean = obj match {
    case b: BitArray => b.array sameElements array
    case _ => false
  }

  def foldLeft[A](initialValue: A)(f: (A, Long) => A): A = {
    var result: A = initialValue
    foreach { value => result = f(result, value) }
    result
  }

  def foreach(f: Long => Unit): Unit = {
    for {
      row <- array.indices
      bits = array(row) if bits != 0
      col <- 0 until bitsPerRow
      value = toValue(row, col) if bits.isOn(col)
    } f(value)
  }

  override def hashCode(): Int = array.hashCode()

  def headOption: Option[Long] = {
    for {
      row <- array.indices
      bits = array(row) if bits != 0
      col <- 0 until bitsPerRow
      value = toValue(row, col) if bits.isOn(col)
    } return Some(value)
    None
  }

  def isEmpty: Boolean = array.forall(_ == 0)

  /**
   * Tests whether the bit at an index within is set
   * @param index the bit's index
   * @return true, if the bit has a value of 1
   */
  def isSet(index: Int): Boolean = {
    val (row, col) = (index / bitsPerRow, index % bitsPerRow)
    (array(row) & setMask(col)) != 0
  }

  def lastOption: Option[Long] = {
    for {
      row <- array.indices.reverse
      bits = array(row) if bits != 0
      col <- (0 until bitsPerRow).reverse
      value = toValue(row, col) if bits.isOn(col)
    } return Some(value)
    None
  }

  def limits: (Long, Long) = (floor, toValue(array.length - 1, bitsPerRow - 1))

  def map[A](f: Long => A): Seq[A] = {
    for {
      row <- array.indices
      bits = array(row) if bits != 0
      col <- 0 until bitsPerRow
      value = toValue(row, col) if bits.isOn(col)
    } yield f(value)
  }

  def max: Option[Long] = {
    for {
      row <- array.indices.reverse
      bits = array(row) if bits != 0
      col <- (0 until bitsPerRow).reverse
    } if (bits.isOn(col)) return Some(toValue(row, col))
    None
  }

  def min: Option[Long] = {
    for {
      row <- array.indices
      bits = array(row) if bits != 0
      col <- 0 until bitsPerRow
    } if (bits.isOn(col)) return Some(toValue(row, col))
    None
  }

  def nonEmpty: Boolean = !isEmpty

  def remove(value: Long): Boolean = {
    val (row, col) = toCoordinates(value)
    if (row >= array.length) false
    else {
      val previous = array(row)
      array(row) &= unsetMask(col)
      val changed = array(row) != previous
      if (changed) notifyChangeListeners()
      changed
    }
  }

  def removeAll(): Unit = {
    for (row <- array.indices) array(row) = 0
    notifyChangeListeners()
  }

  def replaceAll(values: Long*): Unit = {
    for (row <- array.indices) array(row) = 0
    floor = if (values.nonEmpty) values.min else floor
    add(values: _*)
  }

  override def returnType: BitArrayType = BitArrayType

  def size: Int = count(_ => true)

  def takeAll(): List[Long] = {
    val values = ascending
    removeAll()
    values
  }

  override def toString: String = s"${getClass.getSimpleName}(${ascending.mkString(", ")})"

  def truncate(value: Long): Unit = {
    val (startingRow, startingCol) = toCoordinates(value)
    if (startingRow < array.length) {
      // partially clear the starting row
      array(startingRow) &= setMask(startingCol) - 1
      // clear all rows beyond the starting row
      for (row <- startingRow + 1 until array.length) array(row) = 0
    }
    notifyChangeListeners()
  }

  private def ensureCoordinates(value: Long): (Int, Int) = {
    val (row, col) = toCoordinates(value)
    if (row >= array.length) growToFitUpperBound(value)
    else if (row < 0) {
      growToFitLowerBound(value)
      return ensureCoordinates(value)
    }
    (row, col)
  }

  private def growToFitLowerBound(value: Long): Unit = {
    val bitArray = BitArray.withRange(value, max || value)
    bitArray.add(value :: ascending.toList: _*)
    floor = value
    array = bitArray.array
  }

  private def growToFitUpperBound(value: Long): Unit = {
    val newSize = computeArraySize(minValue = floor, maxValue = value + 1)
    if (newSize > array.length) {
      val newArray = new Array[Long](newSize)
      System.arraycopy(array, 0, newArray, 0, array.length)
      array = newArray
    }
  }

  private def notifyChangeListeners(): Unit = {
    changeListeners.foreach(_.changed(this))
  }

  private def overwrite(src: Array[Long]): BitArray = {
    System.arraycopy(src, 0, array, 0, src.length)
    notifyChangeListeners()
    this
  }

  private def toCoordinates(value: Long): (Int, Int) = (value - floor) ~> { v => ((v / bitsPerRow).toInt, (v % bitsPerRow).toInt) }

  override def toTable(implicit scope: Scope): RowCollection = {
    val out = createQueryResultTable(toTableType.columns)
    foreach { rowID =>
      out.insert(Map("rowID" -> rowID).toRow(out))
    }
    out
  }

  override def toTableType: TableType = TableType(columns = Seq(TableColumn(name = "rowID", `type` = Int64Type)))

  private def toValue(row: Int, col: Int): Long = floor + row * bitsPerRow + col

}

object BitArray extends Decoder[BitArray] {
  private val bitsPerRow = 64

  /**
   * Creates a new bit array that will initially contain the provided values
   * @param values the provided values
   * @return a new [[BitArray bit array]]
   */
  def apply(values: Long*): BitArray = {
    val maxValue = if (values.nonEmpty) values.max else 0
    val bitArray = BitArray.withRange(0L, maxValue + 1)
    bitArray.add(values: _*)
    bitArray
  }

  /**
   * @param rows the storage capacity of the bit array (array length)
   * @return the size required to serialize the bit array
   */
  def encodedSize(rows: Int): Int = LONG_BYTES + INT_BYTES + rows * LONG_BYTES

  /**
   * Creates a new bit array declaring the approximate maximum acceptable value. NOTE: it is possibly in using
   * this constructor to produce a bitArray capable of storing values larger that the declared maximum.
   * @param maxValue the approximate upper bound of the acceptable value range
   * @return a new [[BitArray bit array]]
   */
  def withMaxValue(maxValue: Long): BitArray = new BitArray(computeArraySize(maxValue))

  /**
   * Creates a new bit array declaring the minimum and maximum range of acceptable values.
   * @param minValue the lower bound of acceptable value range
   * @param maxValue the upper bound of acceptable value range
   * @return a new [[BitArray bit array]]
   */
  def withRange(minValue: Long, maxValue: Long): BitArray = new BitArray(computeArraySize(maxValue, minValue), minValue)

  def decode(buf: ByteBuffer): BitArray = {
    val minValue = buf.getLong
    val arraySize = buf.getInt
    val src = for (_ <- (0 until arraySize).toArray) yield buf.getLong
    new BitArray(arraySize, minValue).overwrite(src)
  }

  def computeArraySize(maxValue: Long, minValue: Long = 0): Int = {
    (maxValue - minValue) ~> { n => (n / bitsPerRow).toInt + (if (n % bitsPerRow > 0) 1 else 0) }
  }

  def setMask(col: Int): Long = 1L << col

  def toBinary(value: Long): String = value.toBinaryString.reverse.padTo(bitsPerRow, '0').reverse

  def unsetMask(col: Int): Long = (Long.MinValue | Long.MaxValue) ^ setMask(col)

  trait ChangeListener {
    def changed(bitArray: BitArray): Unit
  }

  final implicit class BitArrayExtensions(val bits: Long) extends AnyVal {
    @inline def isOn(col: Int): Boolean = (bits & setMask(col)) != 0
  }

}