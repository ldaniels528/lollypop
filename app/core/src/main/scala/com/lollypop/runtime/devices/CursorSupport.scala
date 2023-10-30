package com.lollypop.runtime.devices

import com.lollypop.runtime.ROWID

/**
 * Cursor and circular-processing support
 */
trait CursorSupport { self: RowCollection =>
  private var position: ROWID = -1

  def hasNext: Boolean = hasNext(position + 1)

  def hasPrevious: Boolean = position > 0

  def get: Row = apply(position max 0)

  def next(): Row = {
    position += 1
    if (!hasNext(position)) position = 0
    apply(position)
  }

  def peek: Option[Row] = if (hasNext) Some(apply(position + 1)) else None

  def previous(): Row = {
    position -= 1
    if (position < 0) position = (getLength - 1) max 0
    apply(position)
  }

  def seek(offset: ROWID): Unit = position = if (hasNext(offset - 1)) offset else self.getLength

}

object CursorSupport {
  def apply(rc: RowCollection): RowCollection with CursorSupport = {
    new AbstractRowCollection with CursorSupport {
      override def out: RowCollection = rc
    }
  }
}
