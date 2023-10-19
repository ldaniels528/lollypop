package com.qwery.runtime.devices

import com.qwery.runtime.ROWID

/**
 * Row-Oriented Support
 */
trait RowOrientedSupport { self: RecordStructure =>

  def fromOffset(offset: Long): ROWID = (offset / recordSize) + ((offset % recordSize) min 1L)

  def toOffset(rowID: ROWID): Long = rowID * recordSize

  def toOffset(rowID: ROWID, columnID: Int): Long = toOffset(rowID) + columnOffsets(columnID)

}
