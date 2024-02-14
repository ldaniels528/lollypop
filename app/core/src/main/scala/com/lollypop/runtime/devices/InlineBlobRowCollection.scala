package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.devices.InlineBlobRowCollection.BLOB_PTR_LENGTH
import com.lollypop.runtime.devices.RowCollection.dieColumnIndexOutOfRange
import lollypop.io.{IOCost, RowIDRange}

import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.ByteBuffer.{allocate, wrap}

/**
 * Represents an Inline BLOB Row Collection
 * @param columns the [[TableColumn table columns]]
 * @param raf     the [[RandomAccessFile]]
 */
class InlineBlobRowCollection(columns: Seq[TableColumn], raf: RandomAccessFile) extends FileRowCollection(columns, raf) {

  override def apply(rowID: ROWID): Row = {
    val metadata = readRowMetadata(rowID)
    Row(rowID, metadata, columns, fields = columns.indices.map(readField(rowID, _)))
  }

  override protected def getMaxPhysicalSize(column: TableColumn): Int = {
    if (column.`type`.isExternal) BLOB_PTR_LENGTH else column.`type`.maxPhysicalSize
  }

  override def insert(row: Row): IOCost = {
    val rowID = getLength
    val buf = allocate(recordSize)
    buf.putRowMetadata(RowMetadata())
    toFieldBuffers(rowID, row.fields).zip(columns) zip columnOffsets foreach {
      case ((fieldBuf, column), offset) if column.isExternal =>
        val (ptrRowID, count) = insertBLOB(startRowID = rowID + 1, fieldBuf)
        buf.position(offset)
        buf.putLong(ptrRowID).putInt(count)
      case ((fieldBuf, column), offset) =>
        putFieldBuffer(column, buf, fieldBuf, offset)
    }
    buf.flipMe()

    // write the buffer
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
    IOCost(inserted = 1, rowIDs = RowIDRange(rowID))
  }

  private def insertBLOB(startRowID: ROWID, buf: ByteBuffer): (ROWID, Int) = {
    // determine the block size
    val blockSize = recordSize - RowMetadata.BYTES_LENGTH
    var block = new Array[Byte](blockSize)
    // put the metadata into the record
    val record = new Array[Byte](recordSize)
    record(0) = RowMetadata(isBlob = true).encode
    // determine the number of rows to be written
    val n_bytes = buf.limit()
    val n_rows = (n_bytes / blockSize) + (if (n_bytes % blockSize > 0) 1 else 0)
    // write the blocks
    for (n <- 0 until n_rows) {
      // extract the block
      buf.position(n * blockSize)
      if (buf.remaining() < block.length) block = new Array[Byte](buf.remaining())
      buf.get(block)
      // copy it into the record
      System.arraycopy(block, 0, record, RowMetadata.BYTES_LENGTH, block.length)
      // write the row meta data
      raf.seek(toOffset(startRowID + n))
      raf.write(record)
    }
    (startRowID, n_bytes)
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val column = columns(columnID)
    val buf = if (column.isExternal) readBLOB(rowID, columnID) else readFixed(rowID, columnID, column.`type`.maxPhysicalSize)
    val (fmd, value_?) = column.`type`.decodeFull(buf)
    Field(name = column.name, fmd, value = value_?)
  }

  private def readFixed(rowID: ROWID, columnID: Int, allocatedSize: Int): ByteBuffer = {
    val bytes = new Array[Byte](allocatedSize)
    raf.seek(toOffset(rowID) + columnOffsets(columnID))
    raf.read(bytes)
    wrap(bytes)
  }

  private def readBLOB(rowID: ROWID, columnID: Int): ByteBuffer = {
    // get the row ID containing the data and the number of bytes
    val (ptrRowID, n_bytes) = {
      val ptrBuf = readFixed(rowID, columnID, BLOB_PTR_LENGTH)
      (ptrBuf.getRowID, ptrBuf.getLength32)
    }
    // compute the block size and number of rows
    val blockSize = recordSize - RowMetadata.BYTES_LENGTH
    val n_rows = (n_bytes / blockSize) + (if (n_bytes % blockSize > 0) 1 else 0)
    var block = new Array[Byte](blockSize)
    val dataBuf = ByteBuffer.allocate(n_bytes)
    // read `n_rows` data blocks (`n_bytes` total bytes)
    for (n <- 0 until n_rows) {
      val offset = toOffset(ptrRowID + n) + 1
      val remaining = n_bytes - (n * blockSize)
      if (remaining < blockSize) block = new Array[Byte](remaining)
      raf.seek(offset)
      raf.read(block)
      dataBuf.position(n * blockSize)
      dataBuf.put(block)
    }
    dataBuf.flipMe()
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    val fields: Seq[ByteBuffer] = toFieldBuffers(rowID, row.fields)
    val buf = allocate(recordSize)
    buf.putRowMetadata(RowMetadata())
    fields.zip(columns) zip columnOffsets foreach { case ((fieldBuf, column), offset) =>
      if (column.isExternal) insertBLOB(startRowID = getLength, fieldBuf)
      else putFieldBuffer(column, buf, fieldBuf, offset)
    }
    buf.flipMe()

    // write the buffer
    raf.seek(toOffset(rowID))
    raf.write(buf.array())
    IOCost(updated = 1)
  }

}

object InlineBlobRowCollection {
  private val BLOB_PTR_LENGTH: Int = LONG_BYTES + INT_BYTES

  def apply(ns: DatabaseObjectNS): InlineBlobRowCollection = {
    new InlineBlobRowCollection(ns.getConfig.columns, new RandomAccessFile(ns.tableDataFile, "rw"))
  }

  def apply(ns: DatabaseObjectNS, columns: Seq[TableColumn]): InlineBlobRowCollection = {
    new InlineBlobRowCollection(columns, new RandomAccessFile(ns.tableDataFile, "rw"))
  }

}
