package com.lollypop.runtime.devices

import com.lollypop.runtime.ROWID
import com.lollypop.runtime.devices.RowCollectionZoo._

/**
 * Pagination support
 */
trait PaginationSupport { self: RowCollection =>
  private var position: ROWID = 0

  def hasNext: Boolean = hasNext(position)

  def hasPrevious: Boolean = position > 0

  def current: Row = apply(position)

  def first(limit: Int): RowCollection = {
    seek(0)
    val rows = if (hasNext(position)) readRange(position, limit) else Nil
    position = (position + limit) min getLength
    createTempTable(columns, rows)
  }

  def last(limit: Int): RowCollection = {
    seek(getLength - limit)
    val rows = if (hasNext(position)) readRange(position, limit) else Nil
    position = (position + limit) min getLength
    createTempTable(columns, rows)
  }

  def next(limit: Int): RowCollection = {
    val rows = if (hasNext(position)) readRange(position, limit) else Nil
    position = (position + limit) min getLength
    createTempTable(columns, rows)
  }

  def previous(limit: Int): RowCollection = {
    val (start, end) = ((position - limit) max 0, position min getLength)
    val rows = readRange(start, numberOfRows = end - start)
    position = start
    createTempTable(columns, rows)
  }

  def seek(offset: ROWID): Unit = position = (getLength min offset) max 0L

  private def readRange(rowID: ROWID, numberOfRows: Long = 1): Seq[Row] = {
    for {
      rowID <- rowID until ((rowID + numberOfRows) min getLength)
      row <- get(rowID)
    } yield row
  }

}

object PaginationSupport {
  def apply(rc: RowCollection): RowCollection with PaginationSupport = {
    new AbstractRowCollection with PaginationSupport with ReadOnlyRowCollection {
      override def out: RowCollection = rc
    }
  }
}
