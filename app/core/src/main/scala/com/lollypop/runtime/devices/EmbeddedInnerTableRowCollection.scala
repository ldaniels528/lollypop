package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.language._
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.errors.EmbeddedWriteOverflowError
import lollypop.io.IOCost
import lollypop.lang.Pointer

import java.io.RandomAccessFile

/**
 * This class facilitates reading and writing rows to an [[RowCollection inner-table]] residing in
 * a [[BlobFile BLOB file]] or within an inner-table in a field of an [[RowCollection outer-table]]
 * @param raf        the [[RandomAccessFile raw file]]
 * @param columns    the [[TableColumn columns]]
 * @param dataBegins the offset of the start of the table within the raw file
 * @param dataEnds   the offset of the end of the table within the raw file
 */
class EmbeddedInnerTableRowCollection(raf: RandomAccessFile, columns: Seq[TableColumn], dataBegins: Long, dataEnds: Long)
  extends FileRowCollection(columns, raf) {
  private val start: Long = dataBegins + TableType.headerSize
  private val allocated = dataEnds - dataBegins
  private var watermark: Long = {
    raf.seek(dataBegins)
    raf.readInt()
  }

  override def insert(row: Row): IOCost = {
    val required = watermark + recordSize
    assert(required <= allocated, throw new EmbeddedWriteOverflowError(required, allocated))
    val cost = super.insert(row)
    updateLimit(cost.rowIDs.headOption || die("No row ID avaiable"))
    cost
  }

  override def close(): Unit = ()

  override def fromOffset(offset: Long): ROWID = (offset - start) ~> { dx => (dx / recordSize) + ((dx % recordSize) min 1L) }

  override def getLength: ROWID = (watermark / recordSize) + ((watermark % recordSize) min 1)

  override def toOffset(rowID: ROWID): Long = start + rowID * recordSize

  override def toOffset(rowID: ROWID, columnID: Int): Long = toOffset(rowID) + columnOffsets(columnID)

  private def updateLimit(rowID: Long): Unit = {
    val proposedLimit = toOffset(rowID + 1) - start
    if (proposedLimit > watermark) {
      watermark = proposedLimit
      raf.seek(dataBegins)
      raf.writeInt(watermark.toInt)
    }
  }

}

/**
 * Embedded Inner-Table Row Collection Companion
 */
object EmbeddedInnerTableRowCollection {

  /**
   * Creates a new Embedded Row Collection
   * @param outerTable the [[FileRowCollection outer-table]]
   * @param rowID      the row ID of the field containing the inner-table
   * @param columnID   the column ID of the field containing the inner-table
   * @return the [[EmbeddedInnerTableRowCollection embedded inner-table]]
   */
  def apply(outerTable: FileRowCollection, rowID: ROWID, columnID: Int): EmbeddedInnerTableRowCollection = {
    // get the details of the outer-table
    val outerTableType = TableType(outerTable.columns)
    val outerTableInnerTableColumn = outerTableType.getColumnByID(columnID)

    // determine the boundary of the inner-table
    val innerTableType = outerTableInnerTableColumn.getTableType
    val innerTableCapacity = innerTableType.maxSizeInBytes / innerTableType.recordSize
    val dataBegins = outerTableType.toOffset(rowID, columnID) + FieldMetadata.BYTES_LENGTH
    val dataEnds = dataBegins + innerTableType.recordSize * innerTableCapacity

    // return the inner-table collection
    new EmbeddedInnerTableRowCollection(outerTable.raf, innerTableType.columns, dataBegins, dataEnds)
  }

  /**
   * Creates a new Embedded Row Collection
   * @param ref      the [[DatabaseObjectRef outer-table reference]]
   * @param rowID    the row ID of the field containing the inner-table
   * @param columnID the column ID of the field containing the inner-table
   * @param scope    the implicit [[Scope scope]]
   * @return the [[EmbeddedInnerTableRowCollection embedded inner-table]]
   */
  def apply(ref: DatabaseObjectRef, rowID: ROWID, columnID: Int)(implicit scope: Scope): EmbeddedInnerTableRowCollection = {
    val outerTableNS = ref.toNS
    val outerTableCfg = outerTableNS.getConfig
    val outerTable = FileRowCollection(columns = outerTableCfg.columns, file = outerTableNS.tableDataFile)
    apply(outerTable, rowID, columnID)
  }

  /**
   * Creates a new Embedded Row Collection via a pointer from a [[BlobFile BLOB file]]
   * @param blobFile the [[BlobFile BLOB file]]
   * @param columns  the [[TableColumn columns]]
   * @param ptr      the [[Pointer pointer reference]]
   * @return the [[EmbeddedInnerTableRowCollection]]
   */
  def apply(blobFile: BlobFile, columns: Seq[TableColumn], ptr: Pointer): EmbeddedInnerTableRowCollection = {
    apply(raf = blobFile.raf, columns, ptr)
  }

  /**
   * Creates a new Embedded Row Collection via a pointer from [[BlobStorage blob storage]]
   * @param raf     the [[RandomAccessFile raw file]]
   * @param columns the [[TableColumn columns]]
   * @param ptr     the [[Pointer pointer reference]]
   * @return the [[EmbeddedInnerTableRowCollection]]
   */
  def apply(raf: RandomAccessFile, columns: Seq[TableColumn], ptr: Pointer): EmbeddedInnerTableRowCollection = {
    new EmbeddedInnerTableRowCollection(raf, columns, ptr.offset, ptr.offset + ptr.allocated)
  }

}
