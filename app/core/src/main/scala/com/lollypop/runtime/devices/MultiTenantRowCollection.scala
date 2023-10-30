package com.lollypop.runtime.devices

import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.{DatabaseObjectNS, ROWID}
import lollypop.io.IOCost
import lollypop.lang.BitArray

/**
 * Represents a Multi-tenant Row Collection
 * @param resource   the shared resource [[RowCollection row collection]]
 * @param visibility the [[BitArray]] that determines the ownership/visibility of rows
 */
class MultiTenantRowCollection(val resource: RowCollection, val visibility: BitArray) extends RowCollection {

  override def columns: Seq[TableColumn] = resource.columns

  override def ns: DatabaseObjectNS = resource.ns

  override def apply(rowID: ROWID): Row = {
    if (visibility.contains(rowID)) resource(rowID) else emptyRow(rowID)
  }

  override def insert(row: Row): IOCost = {
    val cost = resource.insert(row)
    val rowID = cost.firstRowID
    visibility.add(rowID)
    cost
  }

  override def close(): Unit = {}

  override def delete(rowID: ROWID): IOCost = {
    if (visibility.contains(rowID) && visibility.remove(rowID)) resource.delete(rowID) else IOCost.empty
  }

  override def getLength: ROWID = resource.getLength

  override def readField(rowID: ROWID, columnID: Int): Field = {
    if (visibility.contains(rowID)) resource.readField(rowID, columnID)
    else Field(name = columns(columnID).name, metadata = FieldMetadata(isActive = false), value = None)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    if (visibility.contains(rowID)) resource.readFieldMetadata(rowID, columnID) else FieldMetadata(isActive = false)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    if (visibility.contains(rowID)) resource.readRowMetadata(rowID) else RowMetadata(isAllocated = false)
  }

  override def setLength(newSize: ROWID): IOCost = {
    val oldSize = visibility.size
    visibility.truncate(newSize)
    IOCost(deleted = oldSize - visibility.size)
  }

  override def sizeInBytes: Long = resource.sizeInBytes

  override def encode: Array[Byte] = {
    val out = createQueryResultTable(columns)
    visibility.foreach { rowID => out.insert(resource(rowID)) }
    out.encode
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    if (visibility.contains(rowID)) resource.update(rowID, row) else IOCost.empty
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    if (visibility.contains(rowID)) resource.updateField(rowID, columnID, newValue) else IOCost.empty
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    if (visibility.contains(rowID)) resource.updateFieldMetadata(rowID, columnID, fmd) else IOCost.empty
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    if (visibility.contains(rowID)) resource.updateRowMetadata(rowID, rmd) else IOCost.empty
  }

  private def emptyRow(rowID: ROWID): Row = {
    Row(rowID, RowMetadata(isAllocated = false), columns, fields = columns.map(c => Field(c.name, metadata = FieldMetadata(), value = None)))
  }

}

object MultiTenantRowCollection {

  def apply(sharedResource: RowCollection, visibility: BitArray): MultiTenantRowCollection = {
    new MultiTenantRowCollection(sharedResource, visibility)
  }

}