package com.qwery.runtime.devices

import com.qwery.runtime.devices.RowCollection.dieColumnIndexOutOfRange
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.{DatabaseObjectNS, DatabaseObjectRef, ROWID, ResourceManager, Scope}
import com.qwery.util.ByteBufferHelper.DataTypeByteBuffer
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.{IOCost, RowIDRange}

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

/**
 * File Row Collection
 * @param ns      the [[DatabaseObjectNS object namespace]]
 * @param columns the collection of [[TableColumn table columns]]
 * @param raf     the [[RandomAccessFile random-access file]]
 */
class FileRowCollection(val ns: DatabaseObjectNS,
                        val columns: Seq[TableColumn],
                        val raf: RandomAccessFile)
  extends RowCollection with RowOrientedSupport {

  def this(ns: DatabaseObjectNS) = this(ns, ns.getConfig.columns, new RandomAccessFile(ns.tableDataFile, "rw"))

  def this(columns: Seq[TableColumn], raf: RandomAccessFile) = this(createTempNS(), columns, raf)

  override def apply(rowID: ROWID): Row = {
    val buf: ByteBuffer = {
      val payload = new Array[Byte](recordSize)
      raf.seek(toOffset(rowID))
      raf.read(payload)
      wrap(payload)
    }
    Row(rowID, metadata = buf.getRowMetadata, columns = columns, fields = toFields(buf))
  }

  override def close(): Unit = {
    ResourceManager.unlink(ns)
    raf.close()
  }

  override def encode: Array[Byte] = {
    val size = raf.length().toInt
    val bytes = new Array[Byte](size)
    raf.seek(0)
    raf.readFully(bytes)
    bytes
  }

  override def getLength: ROWID = fromOffset(raf.length())

  override def insert(record: Row): IOCost = {
    val rowID = getLength
    update(rowID, record)
    IOCost(inserted = 1, rowIDs = RowIDRange(rowID))
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val column = columns(columnID)
    val buf: ByteBuffer = {
      val offset = columnOffsets(columnID)
      val bytes = new Array[Byte](getMaxPhysicalSize(column))
      raf.seek(toOffset(rowID) + offset)
      raf.read(bytes)
      wrap(bytes)
    }
    val (fmd, value_?) = column.`type`.decodeFull(buf)
    Field(name = column.name, fmd, value = value_?)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    raf.seek(toOffset(rowID, columnID))
    FieldMetadata.decode(raf.read().toByte)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    raf.seek(toOffset(rowID))
    RowMetadata.decode(raf.read().toByte)
  }

  override def setLength(newSize: ROWID): IOCost = {
    val delta = (getLength - newSize) max 0
    if (newSize >= 0 && newSize < raf.length()) raf.setLength(toOffset(newSize))
    IOCost(deleted = delta.toInt)
  }

  override def sizeInBytes: Long = raf.length()

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val column = columns(columnID)
    val buf = column.`type`.encodeFull(newValue ?? column.defaultValue)
    raf.seek(toOffset(rowID, columnID))
    raf.write(buf.array())
    IOCost(updated = 1)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    raf.seek(toOffset(rowID, columnID))
    raf.write(fmd.encode)
    IOCost(updated = 1)
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    val buf = toRowBuffer(toFieldBuffers(rowID, row.fields))
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
    IOCost(updated = 1)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    raf.seek(toOffset(rowID))
    raf.write(rmd.encode)
    IOCost(updated = 1)
  }

}

/**
 * File Row Collection Companion
 */
object FileRowCollection {

  def apply(columns: Seq[TableColumn]): FileRowCollection = apply(createTempNS(columns))

  def apply(columns: Seq[TableColumn], file: File): FileRowCollection = {
    new FileRowCollection(createTempNS(), columns, new RandomAccessFile(file, "rw"))
  }

  def apply(ns: DatabaseObjectNS): FileRowCollection = new FileRowCollection(ns)

  def apply(ref: DatabaseObjectRef)(implicit scope: Scope): FileRowCollection = apply(ns = ref.toNS)

}
