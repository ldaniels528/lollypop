package com.lollypop.runtime.devices

import com.lollypop.runtime.ROWID

/**
 * Creates a lazily-evaluated row collection
 * @param out  the materialized collection; where realized [[Row rows]] will be read.
 * @param rows the [[Iterator iterator]] of [[Row rows]]
 */
class LazyRowCollection(val out: RowCollection, rows: Iterator[Row]) extends AbstractRowCollection with ReadOnlyRowCollection {
  private var watermark: ROWID = 0

  override def apply(rowID: ROWID): Row = {
    if (rowID >= watermark) {
      while ((watermark <= rowID) & rows.hasNext) doNext()
    }
    out.apply(rowID)
  }

  override def getLength: ROWID = {
    while (rows.hasNext) doNext()
    watermark
  }

  override def hasNext(rowID: ROWID): Boolean = (rowID < watermark) | rows.hasNext

  private def doNext(): Unit = {
    out.update(watermark, rows.next())
    watermark += 1
  }

}

object LazyRowCollection{

  def apply(out: RowCollection, rows: Iterator[Row]): RowCollection = {
    val rc = new LazyRowCollection(out, rows)
    new HostedRowCollection {
      override def host: RowCollection = rc
    }
  }

  def apply(out: RowCollection, rows: LazyList[Row]): RowCollection = {
    val rc = new LazyRowCollection(out, rows.iterator)
    new HostedRowCollection {
      override def host: RowCollection = rc
    }
  }

}