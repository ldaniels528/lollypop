package com.qwery.runtime.devices

import com.qwery.runtime.{ROWID, Scope}
import qwery.io.IOCost

trait WrappedRecordCollection[T] extends RecordCollection[T] {

  def host: RecordCollection[T]

  override def insert(record: T): IOCost = host.insert(record)

  override def apply(rowID: ROWID): T = host(rowID)

  override def close(): Unit = host.close()

  override def columns: Seq[TableColumn] = host.columns

  override def distinct: RecordCollection[T] = host.distinct

  override def get(rowID: ROWID): Option[T] = host.get(rowID)

  override def getRowScope(rowID: ROWID)(implicit scope: Scope): Scope = host.getRowScope(rowID)

  override def getLength: Long = host.getLength

  override def isMemoryResident: Boolean = host.isMemoryResident

  override def push(item: Any): IOCost = host.push(item)

  override def readField(rowID: ROWID, columnID: Int): Field = host.readField(rowID, columnID)

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = host.readFieldMetadata(rowID, columnID)

  override def readRowMetadata(rowID: ROWID): RowMetadata = host.readRowMetadata(rowID)

  override def setLength(newSize: ROWID): IOCost = host.setLength(newSize)

  override def sizeInBytes: Long = host.sizeInBytes

  override def slice(rowID0: ROWID, rowID1: ROWID): RecordCollection[T] = host.slice(rowID0, rowID1)

  override def encode: Array[Byte] = host.encode

  override def toMap(row: T): Map[String, Any] = host.toMap(row)

  override def update(rowID: ROWID, row: T): IOCost = host.update(rowID, row)

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    host.updateField(rowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    host.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    host.updateRowMetadata(rowID, rmd)
  }

}
