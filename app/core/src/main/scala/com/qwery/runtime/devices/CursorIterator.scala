package com.qwery.runtime.devices

import com.qwery.language.models.Condition
import com.qwery.runtime.devices.CursorIterator.CursorIteratorRowCollection
import com.qwery.runtime.errors.IteratorEmptyError
import com.qwery.runtime.instructions.conditions.RuntimeCondition
import com.qwery.runtime.{ROWID, Scope}

/**
 * Represents a Cursor Iterator
 * @param host    the host [[RowCollection]]
 * @param where the [[Condition]]
 * @param scope the implicit [[Scope scope]]
 */
case class CursorIterator(host: RowCollection, where: Option[Condition] = None)(implicit scope: Scope)
  extends HostedRowCollection {
  private var row_? : Option[Row] = None
  private var rowID: ROWID = -1

  def hasNext: Boolean = {
    val (newPosition, newRow_?) = host.findMatch(rowID + 1, isForward = true, where)
    rowID = newPosition
    row_? = newRow_?
    row_?.nonEmpty
  }

  def hasPrevious: Boolean = {
    val (newPosition, newRow_?) = host.findMatch(rowID - 1, isForward = false, where)
    rowID = newPosition
    row_? = newRow_?
    row_?.nonEmpty
  }

  def next(): Row = row_? match {
    case Some(row) => row_? = None; row
    case None => throw new IteratorEmptyError()
  }

  def peek: Option[Row] = host.findMatch(rowID, isForward = false, where)._2

  def previous(): Row = row_? match {
    case Some(row) => row_? = None; row
    case None => throw new IteratorEmptyError()
  }

  override def toMapGraph: List[Map[String, Any]] = {
    // save the current state
    val (savedRowID, savedRow_?) = (rowID, row_?)

    // build the graph
    rowID = -1
    var graph: List[Map[String, Any]] = Nil
    while (hasNext) {
      graph = graph ::: next().toMap :: Nil
    }

    // restore the state
    rowID = savedRowID
    row_? = savedRow_?

    // return the graph
    graph
  }

}

/**
 * Cursor Iterator Companion
 */
object CursorIterator {

  final implicit class CursorIteratorRowCollection(val rc: RowCollection) extends AnyVal {

    @inline
    def cursor(where: Option[Condition] = None)(implicit scope: Scope): CursorIterator = CursorIterator(rc, where)

    def findMatch(fromPos: ROWID, isForward: Boolean, where: Option[Condition] = None)(implicit scope: Scope): (ROWID, Option[Row]) = {
      var rowID: ROWID = fromPos
      val step = if (isForward) 1 else -1

      def hasMore: Boolean = if (isForward) rc.hasNext(rowID) else rowID >= 0

      while (hasMore) {
        val rowScope = rc.getRowScope(rowID)
        if (where.isEmpty || where.exists(RuntimeCondition.isTrue(_)(rowScope))) return (rowID, rowScope.getCurrentRow)
        rowID += step
      }
      (rowID, None)
    }

  }

}