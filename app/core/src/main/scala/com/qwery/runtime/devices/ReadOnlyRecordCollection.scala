package com.qwery.runtime.devices

import com.qwery.language.dieTableIsReadOnly
import com.qwery.runtime.ROWID
import qwery.io.IOCost

/**
 * Represents a read-only row collection support
 */
trait ReadOnlyRecordCollection[T] { self: RecordCollection[T] =>

  override def insert(record: T): IOCost = dieTableIsReadOnly()

  override def setLength(newSize: ROWID): IOCost = dieTableIsReadOnly()

  override def update(rowID: ROWID, row: T): IOCost = dieTableIsReadOnly()

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = dieTableIsReadOnly()

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = dieTableIsReadOnly()

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = dieTableIsReadOnly()

}
