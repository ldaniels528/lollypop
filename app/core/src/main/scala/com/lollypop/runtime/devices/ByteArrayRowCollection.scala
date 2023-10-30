package com.lollypop.runtime.devices

import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.RowCollection.dieColumnIndexOutOfRange
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.{DatabaseObjectNS, ROWID}
import com.lollypop.util.ByteBufferHelper.DataTypeByteBuffer
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.{IOCost, RowIDRange}

import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

/**
 * Represents a byte-array backed row collection
 * @param ns           the [[DatabaseObjectNS table namespace]]
 * @param columns      the collection of [[TableColumn table columns]]
 * @param array        the byte-array
 * @param growthFactor the growth factor
 */
class ByteArrayRowCollection(val ns: DatabaseObjectNS,
                             val columns: Seq[TableColumn],
                             var array: Array[Byte],
                             val growthFactor: Double = 0.20)
  extends MemoryRowCollection with RowOrientedSupport {
  // the EOF (offset)
  var watermark: Long = 0L

  override def insert(record: Row): IOCost = {
    val rowID = nextRowID
    update(rowID, record)
    IOCost(inserted = 1, rowIDs = RowIDRange(rowID))
  }

  override def close(): Unit = {}

  override def encode: Array[Byte] = array

  override def getLength: ROWID = fromOffset(watermark)

  override def readField(rowID: ROWID, columnID: Int): Field = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val column = columns(columnID)
    val buf: ByteBuffer = {
      val p0 = toOffset(rowID, columnID).toInt
      val bytes = new Array[Byte](getMaxPhysicalSize(column))
      System.arraycopy(array, p0, bytes, 0, bytes.length)
      wrap(bytes)
    }
    val (fmd, value_?) = column.`type`.decodeFull(buf)
    Field(name = column.name, fmd, value = value_?)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    FieldMetadata.decode(array(toOffset(rowID, columnID).toInt))
  }

  override def apply(rowID: ROWID): Row = {
    val buf: ByteBuffer = {
      val p0 = toOffset(rowID).toInt
      val bytes = new Array[Byte](recordSize)
      System.arraycopy(array, p0, bytes, 0, bytes.length)
      wrap(bytes)
    }
    Row(rowID, metadata = buf.getRowMetadata, columns = columns, fields = toFields(buf))
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    RowMetadata.decode(array(toOffset(rowID).toInt))
  }

  override def setLength(newSize: ROWID): IOCost = {
    val delta = (getLength - newSize) max 0
    watermark = toOffset(newSize max 0)
    IOCost(deleted = delta.toInt)
  }

  override def sizeInBytes: Long = array.length

  override def update(rowID: ROWID, row: Row): IOCost = {
    val buf = toRowBuffer(toFieldBuffers(rowID, row.fields))
    val offset = toOffset(rowID).toInt
    val required = offset + 1
    ensureCapacity(required)
    val bytes = buf.array()
    System.arraycopy(bytes, 0, array, offset, bytes.length)
    watermark = Math.max(watermark, required)
    IOCost(updated = 1)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val column = columns(columnID)
    val buf = column.`type`.encodeFull(newValue ?? column.defaultValue)
    val offset = toOffset(rowID, columnID).toInt
    val required = offset + 1
    ensureCapacity(required)
    val bytes = buf.array()
    System.arraycopy(bytes, 0, array, offset, bytes.length)
    watermark = Math.max(watermark, required)
    IOCost(updated = 1)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    val offset = toOffset(rowID, columnID).toInt
    val required = offset + 1
    ensureCapacity(required)
    array(offset) = fmd.encode
    watermark = Math.max(watermark, required)
    IOCost(updated = 1)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    val offset = toOffset(rowID).toInt
    val required = offset + 1
    ensureCapacity(required)
    array(offset) = rmd.encode
    watermark = Math.max(watermark, required)
    IOCost(updated = 1)
  }

  private def ensureCapacity(offset: Int): Unit = {
    if (offset >= array.length) {
      val newSize = (((offset / recordSize) + ((offset % recordSize) max 1)) * (1 + growthFactor)).toInt * recordSize
      val newArray = new Array[Byte](newSize)
      System.arraycopy(array, 0, newArray, 0, array.length)
      array = newArray
    }
  }

  private def nextRowID: ROWID = fromOffset(watermark)

}

object ByteArrayRowCollection {

  /**
   * Creates a new byte array-backed in memory table
   * @param tableType the [[TableType table type]]
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(tableType: TableType): ByteArrayRowCollection = apply(createTempNS(), tableType)

  /**
   * Creates a new byte array-backed in memory table
   * @param columns  the [[TableColumn table columns]]
   * @param capacity the initial capacity
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(columns: Seq[TableColumn], capacity: Int): ByteArrayRowCollection = {
    apply(columns, capacity, growthFactor = 0.20)
  }

  /**
   * Creates a new byte array-backed in memory table
   * @param columns      the [[TableColumn table columns]]
   * @param capacity     the initial capacity
   * @param growthFactor the growth factor
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(columns: Seq[TableColumn], capacity: Int, growthFactor: Double): ByteArrayRowCollection = {
    apply(createTempNS(), columns: Seq[TableColumn], capacity, growthFactor)
  }

  /**
   * Creates a new byte array-backed in memory table
   * @param ns        the [[DatabaseObjectNS table namespace]]
   * @param tableType the [[TableType table type]]
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(ns: DatabaseObjectNS, tableType: TableType): ByteArrayRowCollection = {
    new ByteArrayRowCollection(ns,
      columns = tableType.columns,
      array = new Array(tableType.capacity * tableType.recordSize),
      growthFactor = tableType.growthFactor)
  }

  /**
   * Creates a new byte array-backed in memory table
   * @param ns           the [[DatabaseObjectNS table namespace]]
   * @param columns      the [[TableColumn table columns]]
   * @param capacity     the initial capacity
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(ns: DatabaseObjectNS, columns: Seq[TableColumn], capacity: Int): ByteArrayRowCollection = {
    apply(ns, columns, capacity, growthFactor = 0.20)
  }

  /**
   * Creates a new byte array-backed in memory table
   * @param ns           the [[DatabaseObjectNS table namespace]]
   * @param columns      the [[TableColumn table columns]]
   * @param capacity     the initial capacity
   * @param growthFactor the growth factor
   * @return the [[ByteArrayRowCollection]]
   */
  def apply(ns: DatabaseObjectNS, columns: Seq[TableColumn], capacity: Int, growthFactor: Double): ByteArrayRowCollection = {
    val structure = RecordStructure(columns)
    new ByteArrayRowCollection(ns, columns, new Array(capacity * structure.recordSize), growthFactor)
  }

}