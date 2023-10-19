package com.qwery.runtime.devices

import com.qwery.runtime.ROWID
import qwery.io.IOCost

/**
 * Inner-tables are decoded as [[MemoryRowCollection]]s and as such updates to them do not carry-over
 * to the persistent storage they were retrieved from. This adapter class remedies that issue by
 * facilitating a write-back mechanism to update the outer table's field where the inner-table is persisted.
 * @param outerTable the table whose column contains the inner-table
 * @param innerTable the inner-table; usually a [[MemoryRowCollection]]
 * @param rowID      the row ID of the record containing the inner-table.
 * @param columnID   the column ID of the record containing the inner-table.
 */
class InnerTableRowCollection(outerTable: RowCollection, innerTable: RowCollection, rowID: ROWID, columnID: Int)
  extends HostedRowCollection {

  override def host: RowCollection = innerTable

  override def insert(record: Row): IOCost = writeBack(innerTable.insert(record))

  override def isMemoryResident: Boolean = false

  override def setLength(newSize: ROWID): IOCost = writeBack(innerTable.setLength(newSize))

  override def update(rowID: ROWID, row: Row): IOCost = writeBack(innerTable.update(rowID, row))

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    writeBack(innerTable.updateField(rowID, columnID, newValue))
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    writeBack(innerTable.updateFieldMetadata(rowID, columnID, fmd))
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    writeBack(innerTable.updateRowMetadata(rowID, rmd))
  }

  private def writeBack(operation: => IOCost): IOCost = {
    val cost = operation
    outerTable.updateField(rowID, columnID, newValue = Option(innerTable))
    cost
  }

}
