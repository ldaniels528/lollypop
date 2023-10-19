package com.qwery.runtime.devices

import com.qwery.runtime.ROWID
import com.qwery.runtime.errors.IteratorEmptyError

/**
 * Represents a Basic Iterator
 * @param host      the [[RecordCollection]]
 * @param isForward indicates whether to move forward (or in reverse) through the iterator
 * @tparam T the template type; typically a [[Row]]
 */
case class BasicIterator[T](host: RecordCollection[T], isForward: Boolean) extends WrappedRecordCollection[T] {
  private val step: Int = if (isForward) 1 else -1
  private var item_? : Option[T] = None
  private var rowID: ROWID = if (isForward) -1 else host.getLength

  def hasNext: Boolean = {
    if (rowID + step < 0) false
    else {
      val index_? = findRow(fromPos = rowID + step, isForward)(_.isActive)
      item_? = index_?.flatMap(host.get)
      index_?.foreach(_ => rowID += step)
      item_?.nonEmpty
    }
  }

  def next(): T = item_? match {
    case Some(item) => item_? = None; item
    case None => throw new IteratorEmptyError()
  }

  def peek: Option[T] = findRow(rowID, isForward)(_.isActive).flatMap(host.get)

  def skip(count: Int): this.type = {
    rowID += count
    this
  }

  private def findRow(fromPos: ROWID, isForward: Boolean)(f: RowMetadata => Boolean): Option[ROWID] = {
    var rowID: ROWID = fromPos

    def isValid: Boolean = if (isForward) hasNext(rowID - 1) else rowID >= 0

    // scan the metadata for the next row matching our criteria
    if (isForward) while (hasNext(rowID) & !f(readRowMetadata(rowID))) rowID += 1
    else while (rowID >= 0 & !f(readRowMetadata(rowID))) rowID -= 1
    if (isValid) Some(rowID) else None
  }

}

/**
 * Basic Iterator Companion
 */
object BasicIterator {

  final implicit class RichBasicIterator[T](val rc: RecordCollection[T]) extends AnyVal {

    @inline
    def iterator: BasicIterator[T] = BasicIterator(rc, isForward = true)

    @inline
    def reverseIterator: BasicIterator[T] = BasicIterator(rc, isForward = false)

  }

}