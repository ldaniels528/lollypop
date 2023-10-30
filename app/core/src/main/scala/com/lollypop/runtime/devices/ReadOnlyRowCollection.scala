package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.runtime.ROWID
import lollypop.io.IOCost

/**
 * Read-Only Row Collection trait
 */
trait ReadOnlyRowCollection { self: RowCollection =>

  override def insert(record: Row): IOCost = dieReadOnly()

  override def pop(): Row = dieReadOnly()

  override def push(item: Any): IOCost = dieReadOnly()

  override def setLength(newSize: ROWID): IOCost = dieReadOnly()

  override def update(rowID: ROWID, row: Row): IOCost = dieReadOnly()

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = dieReadOnly()

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = dieReadOnly()

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = dieReadOnly()

  private def dieReadOnly(): Nothing = die("This row collection is read-only")

}

/**
 * Read-Only Row Collection companion
 */
object ReadOnlyRowCollection {

  def apply(rc: RowCollection): RowCollection = new AbstractRowCollection with ReadOnlyRowCollection {
    override def out: RowCollection = rc
  }

}
