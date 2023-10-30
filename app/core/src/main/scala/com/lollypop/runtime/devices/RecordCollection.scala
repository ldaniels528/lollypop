package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.runtime.{ROWID, Scope}
import com.lollypop.util.Tabulator
import lollypop.io.IOCost

/**
 * Represents a Record Collection
 * @tparam T the data object type
 */
trait RecordCollection[T] extends RecordMetadataIO {

  /**
   * Retrieves the item corresponding to the record offset
   * @param rowID the record offset
   * @return the [[T item]]
   */
  def apply(rowID: ROWID): T

  /**
   * Completely removes deleted rows from of this device; re-ordering the rows in the process
   * @return the new size of the file
   */
  def compact(): IOCost = {
    var bottom: ROWID = getLength - 1
    var rowID: ROWID = 0
    var cc = IOCost()
    while (rowID < bottom) {
      if (!readRowMetadata(rowID).isActive) {
        cc = cc ++ IOCost(scanned = 1)
        // find the last active record
        while (bottom > rowID && !readRowMetadata(bottom).isActive) bottom -= 1
        if (rowID < bottom) {
          cc = cc ++ update(rowID, apply(bottom))
          bottom -= 1
        }
      }
      rowID += 1
    }
    cc ++ setLength(bottom + 1)
  }

  /**
   * Removes deleted rows from of this device from the top down; re-ordering the rows in the process
   * @return the new size of the file
   */
  def compactLeft(rowID: ROWID = 0, bottom: ROWID = getLength - 1): IOCost = {
    // read a dead row and overwrite it with a row from the bottom of the file
    if (rowID < bottom && readRowMetadata(rowID).isDeleted) {
      update(rowID, apply(bottom)) ++ IOCost(scanned = 1) ++ compactLeft(rowID + 1, bottom - 1)
    } else {
      setLength(bottom + 1)
    }
  }

  /**
   * Removes deleted rows from of this device from the bottom up; re-ordering the rows in the process
   * @return the new size of the file
   */
  def compactRight(bottom: ROWID = getLength - 1): IOCost = {
    // first trim all dead rows from the bottom of the file
    if (bottom >= 0 && readRowMetadata(bottom).isDeleted) compactRight(bottom - 1) else setLength(bottom + 1)
  }

  def deleteSlice(rowID0: ROWID, rowID1: ROWID): IOCost = {
    (rowID0 to rowID1).foldLeft(IOCost())((agg, id) => agg ++ delete(id))
  }

  /**
   * @return a new [[RecordCollection device]] containing only distinct rows
   */
  def distinct: RecordCollection[T]

  def foldLeft[A](initialValue: A)(f: (A, T) => A): A = {
    var result: A = initialValue
    foreach { row => result = f(result, row) }
    result
  }

  def foreach[U](callback: T => U): Unit = {
    var rowID = 0L
    while (hasNext(rowID)) {
      get(rowID).foreach(callback)
      rowID += 1
    }
  }

  def get(rowID: ROWID): Option[T]

  /**
   * Retrieves a row by ID
   * @param rowID the row ID
   * @return the [[Scope scope]] containing the current row
   */
  def getRowScope(rowID: ROWID)(implicit scope: Scope): Scope

  /**
   * Appends the collection of items to the end of this collection
   * @param records the [[T records]] to append
   */
  def insert(records: Iterable[T]): IOCost = records.foldLeft(IOCost())((agg, row) => agg ++ insert(row))

  /**
   * Appends the collection of items to the end of this collection
   * @param rows the [[T rows]] to append
   */
  def insert(rows: RecordCollection[T]): IOCost = rows.foldLeft(IOCost())((agg, row) => agg ++ insert(row))

  /**
   * Appends an item to the end of this collection
   * @param record the [[T record]] to append
   */
  def insert(record: T): IOCost

  def map[U](predicate: T => U): List[U] = {
    var out: List[U] = Nil
    foreach(row => out = out ::: predicate(row) :: Nil)
    out
  }

  def pop(): T = {
    lastIndexWhereMetadata()(_.isActive) match {
      case Some(rowID) =>
        val item = apply(rowID)
        delete(rowID)
        item
      case None => die("No rows available")
    }
  }

  def push(item: Any): IOCost

  def reverseInPlace(top: ROWID = 0L, bottom: ROWID = getLength - 1): IOCost = {
    if (top < bottom) swap(top, bottom) ++ reverseInPlace(top + 1, bottom - 1) else IOCost()
  }

  /**
   * Retrieves a range of rows as a new block device
   * @param rowID0 the initial row ID of the range
   * @param rowID1 the last row ID of the range
   * @return a [[RecordCollection device]] containing the rows
   */
  def slice(rowID0: ROWID, rowID1: ROWID): RecordCollection[T]

  def swap(rowID0: ROWID, rowID1: ROWID): IOCost = {
    val (block0, block1) = (apply(rowID0), apply(rowID1))
    update(rowID0, block1) ++ update(rowID1, block0) ++ IOCost(scanned = 2)
  }

  def tabulate(limit: Int = Int.MaxValue): List[String] = {
    Tabulator.tabulate(headers = columns.map(_.name).toList, rows = toValueLists, limit)
  }

  def toList: List[T] = foldLeft[List[T]](Nil) { (list, row) => list ::: row :: Nil }

  def toMap(row: T): Map[String, Any]

  /**
   * @return a list-map representation of this device
   */
  def toMapGraph: List[Map[String, Any]] = {
    def mapify(rc: RecordCollection[T]): List[Map[String, Any]] = {
      rc.toList.map(toMap(_).flatMap {
        case (_, None) => None
        case (name, Some(value)) => Some(name -> value)
        case (name, device: RecordCollection[T]) => Some(name -> mapify(device))
        case x => Some(x)
      })
    }

    mapify(this)
  }

  def toValueLists: List[List[Option[Any]]] = {
    val columnNames = columns.map(_.name).toList
    val rows = foldLeft[List[Map[String, Any]]](Nil) { (list, row) => list ::: toMap(row) :: Nil }
    rows.map(m => columnNames.map(m.get))
  }

  def update(rowID: ROWID, row: T): IOCost

  protected def _indexOf[U](isDone: ROWID => Boolean, fromPos: ROWID = 0, toPos: ROWID = getLength)(f: (ROWID, T) => U): IOCost = {
    var (rowID: ROWID, matched, scanned) = (fromPos, 0, 0)
    while (rowID < toPos && !isDone(rowID)) {
      get(rowID).foreach { row =>
        matched += 1
        f(rowID, row)
      }
      scanned += 1
      rowID += 1
    }
    IOCost(scanned = scanned, matched = matched)
  }

}
