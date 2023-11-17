package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.language.models.Expression.implicits.RichAliasable
import com.lollypop.language.models.{AllFields, Condition, Expression, ScopeModification}
import com.lollypop.language.{dieIllegalType, dieNoSuchColumn}
import com.lollypop.runtime.LollypopVM.implicits.{InstructionExtensions, RichScalaAny}
import com.lollypop.runtime._
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.datatypes.{IBLOB, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollection.getDurableInnerTable
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.RichConditionAtRuntime
import com.lollypop.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.lollypop.runtime.instructions.conditions.{RuntimeCondition, WhereIn}
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.queryables.TableRendering
import com.lollypop.util.DateHelper
import com.lollypop.util.JVMSupport.NormalizeAny
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper.AutoClose
import lollypop.io.IOCost
import lollypop.lang.Pointer

import java.io.{File, FileWriter, PrintWriter}
import java.util.{Date, UUID}
import scala.collection.mutable
import scala.util.Random

/**
 * Represents a row-based record collection
 */
trait RowCollection extends RecordCollection[Row] with DataObject with SQLSupport
  with TableExpression with TableRendering with AutoCloseable {

  def +(that: RowCollection): RowCollection = union(that)

  def -(that: RowCollection): RowCollection = subtract(that)

  def *(that: RowCollection): RowCollection = product(that)

  def /(columnNames: Array[String]): RowCollection = partitionBy(columnNames)

  def %(columnNames: Array[String]): RowCollection = columnSlice(columnNames)

  def |(that: RowCollection): RowCollection = unionDistinct(that)

  def &(that: RowCollection): RowCollection = intersect(that)

  override def apply(rowID: ROWID): Row

  def columnSlice(columnNames: Array[String]): RowCollection = {
    val columns = for {column <- this.columns if columnNames.contains(column.name)} yield column
    val out = if (isMemoryResident) createQueryResultTable(columns) else createTempTable(columns)
    foreach { row =>
      val outRow = row.copy(columns = columns, fields = row.fields.filter(f => columnNames.contains(f.name)))
      out.insert(outRow)
    }
    out
  }

  /**
   * Counts the number of rows matching the optional criteria
   * @param condition the optional [[Condition criteria]]
   * @param limit     the maximum number of records to count
   * @return the number of rows matching the optional criteria
   */
  def countWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    iterateWhere(condition, limit)(_.isActive) { (_, _) => }
  }

  /**
   * Removes rows from an inner-table matching the given criteria (up to the optionally specified limit)
   * @param innerTableColumn the [[TableColumn column]] that contains the inner-table
   * @param condition        the deletion [[Condition criterion]]
   * @param limit            the maximum number of records to delete
   * @return the number of rows deleted
   * @example {{{
   *  delete from stocks#transactions
   *  where symbol is 'SHMN'
   *  and wherein transactions (price is 0.001)
   * }}}
   */
  def deleteInside(innerTableColumn: TableColumn,
                   condition: Option[Condition],
                   limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    iterateWhere(condition, limit)(_.isActive) { (outerRowScope, outerRow) =>
      outerRow.getField(innerTableColumn.name).flatMap(_.value) collect {
        case innerTable: RowCollection =>
          val innerCondition = getInnerTableCondition(innerTableColumn, condition)
          val out = getDurableInnerTable(outerTable = this, innerTable, innerTableColumn, outerRow)
          out.iterateWhere(innerCondition)(_.isActive) { case (_, innerRow) => out.delete(innerRow.id) }(outerRowScope)
      }
    }
  }

  /**
   * Deletes rows matching the given criteria (up to the optionally specified limit)
   * @param condition the deletion criteria
   * @param limit     the maximum number of records to delete
   * @return the number of rows deleted
   * @example {{{
   *  delete from stocks where symbol is 'SHMN'
   * }}}
   */
  def deleteWhere(condition: Option[Condition], limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    this.iterateWhere(condition, limit)(_.isActive) { case (_, row) => delete(row.id) }
  }

  override def distinct: RowCollection = {
    val set = mutable.HashSet[Seq[Field]]()
    val out = createQueryResultTable(columns)
    this foreach { row =>
      if (!set.contains(row.fields)) {
        set += row.fields
        out.insert(row)
      }
    }
    out
  }

  def exists(f: Row => Boolean): Boolean = {
    var found = false
    var id = 0L
    val eof = getLength
    while (!found && id < eof) {
      val row = apply(id)
      if (row.metadata.isActive) found = f(row)
      id += 1
    }
    found
  }

  /**
   * Exports the contents of this device as Comma Separated Values (CSV)
   * @return the [[IOCost cost]]
   */
  def exportAsCSV(file: String): IOCost = exportAsCSV(new File(file))

  /**
   * Exports the contents of this device as Comma Separated Values (CSV)
   * @return the [[IOCost cost]]
   */
  def exportAsCSV(file: File): IOCost = {
    val columnNames = columns.map(_.name)

    def toCsvText(row: Row): String = {
      val mappings = mapForText(row, isJSON = false)
      (for {name <- columnNames; value <- mappings.get(name)} yield value).mkString(",")
    }

    new PrintWriter(new FileWriter(file)) use { out =>
      // write the header
      out.println(columnNames.map(s => s"\"$s\"").mkString(","))
      // write all rows
      writeFile(out, toCsvText)
    }
  }

  /**
   * Exports the contents of this device as JavaScript Object Notation (JSON)
   * @return the [[IOCost cost]]
   */
  def exportAsJSON(file: String): IOCost = exportAsJSON(new File(file))

  /**
   * Exports the contents of this device as JavaScript Object Notation (JSON)
   * @return the [[IOCost cost]]
   */
  def exportAsJSON(file: File): IOCost = {

    def toJSONText(row: Row): String = {
      s"{ ${mapForText(row, isJSON = true) map { case (k, v) => s""""$k":$v""" } mkString ","} }"
    }

    new PrintWriter(new FileWriter(file)) use { out =>
      writeFile(out, toJSONText)
    }
  }

  private def mapForText(row: Row, isJSON: Boolean): Map[String, Any] = {
    Map(row.fields collect {
      case Field(name, _, None) => name -> "null"
      case Field(name, _, Some(value: Date)) => name -> s""""${DateHelper.format(value)}""""
      case Field(name, _, Some(value: Date)) if isJSON => name -> value.getTime
      case Field(name, _, Some(value: String)) => name -> s""""$value""""
      case Field(name, _, Some(value: UUID)) => name -> s""""$value""""
      case Field(name, _, Some(value)) => name -> value.toString
    }: _*)
  }

  private def writeFile(out: PrintWriter, convert: Row => String): IOCost = {
    var count = 0
    foreach { row =>
      out.println(convert(row))
      count += 1
    }
    IOCost(scanned = count)
  }

  override def get(rowID: ROWID): Option[Row] = {
    val row = apply(rowID)
    if (row.metadata.isActive) Some(row) else None
  }

  override def getRowScope(rowID: ROWID)(implicit scope: Scope): Scope = scope.withCurrentRow(row = Some(apply(rowID)))

  def indexOf(condition: QMap[String, Any]): Option[ROWID] = {

    def matches(values: QMap[String, Any], row: Row): Boolean = {
      val mappings = row.toMap
      values.forall { case (name, value) => mappings.get(name).contains(value) }
    }

    var rowID: ROWID = 0
    while (hasNext(rowID)) {
      val rmd = readRowMetadata(rowID)
      if (rmd.isActive) {
        val row = apply(rowID)
        if (matches(condition, row)) return Some(rowID)
      }
      rowID += 1
    }
    None
  }

  def insert(rows: RowCollection): IOCost = {
    rows.foldLeft(IOCost())((agg, row) => agg ++ insert(row.toRow(this)))
  }

  /**
   * Appends new rows into an inner table within this device
   * @param innerTableColumn the inner-table [[TableColumn column]]
   * @param injectColumns    the column names
   * @param injectValues     the collection of [[Expression expressions]] representing the insert rows
   * @param condition        the [[Condition selection criterion]]
   * @param limit            the limit [[Expression expression]]
   * @param scope            the implicit [[Scope scope]]
   * @return the collection of inserted row IDs
   * @example
   * {{{
   * insert into stocks#transactions (price, transactionTime)
   * values (35.11, "2021-08-05T19:23:12.000Z"),
   *        (35.83, "2021-08-05T19:23:15.000Z"),
   *        (36.03, "2021-08-05T19:23:17.000Z")
   * where symbol is 'AMD'
   * }}}
   */
  def insertInside(innerTableColumn: TableColumn,
                   injectColumns: Seq[String],
                   injectValues: Seq[Seq[Expression]],
                   condition: Option[Condition],
                   limit: Option[Expression])(implicit scope: Scope): IOCost = {
    val tableType = innerTableColumn.getTableType
    iterateWhere(condition, limit)(_.isActive) { (scope, outerRow) =>
      outerRow.getField(innerTableColumn.name).flatMap(_.value).map(tableType.convert) match {
        case Some(innerTable) =>
          val out = getDurableInnerTable(outerTable = this, innerTable, innerTableColumn, outerRow)
          out.insertRows(injectColumns, injectValues)(scope)
        case None => IOCost()
      }
    }
  }

  /**
   * Appends new rows to this device
   * @param columns   the column names
   * @param rowValues the collection of [[Expression expressions]] representing the insert rows
   * @param scope     the implicit [[Scope scope]]
   * @return the collection of inserted row IDs
   * @example
   * {{{
   * insert into Tickers (symbol, exchange, lastSale)
   * values ('AAPL', 'NASDAQ', 145.67), ('AMD', 'NYSE', 5.66)
   * }}}
   */
  def insertRows(columns: Seq[String], rowValues: Seq[Seq[Expression]])(implicit scope: Scope): IOCost = {
    (for {
      values <- rowValues.map(_.map(_.execute(scope)._3))
      row = Map(columns zip values map { case (column, value) => column -> value }: _*)
    } yield insert(row.toRow(this))).reduce(_ ++ _)
  }

  /**
   * Returns a collection that represents the intersection between the host collection and another collection
   * @param that the other [[RowCollection collection]]
   * @return the [[RowCollection intersection]] of the two collections
   */
  def intersect(that: RowCollection): RowCollection = {
    verifyColumnsMatch(that)
    var rowID: ROWID = 0
    val out = createQueryResultTable(columns)
    val set = mutable.HashSet[Seq[Field]]() // TODO use hash index instead
    val (device0, device1) = if (getLength < that.getLength) (this, that) else (that, this)
    device0.foreach { row => set += row.fields }
    device1.foreach { row =>
      if (set.contains(row.fields)) {
        out.update(rowID, row)
        rowID += 1
      }
    }
    out
  }

  /**
   * Iterates rows matching the given condition up to the limit of rows
   * @param condition  the [[Condition condition]]
   * @param limit      the [[Expression limit]]
   * @param includeRow the function that determines via metadata whether a row will be processed
   * @param process    the function to process selected rows
   * @param scope      the implicit [[Scope scope]]
   * @return the resulting [[IOCost cost]] of the entire operation
   */
  def iterateWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)
                  (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    var rowID: ROWID = 0
    var cost = IOCost()
    val _limit = limit.map(_.pullInt._3)
    while (hasNext(rowID) & (_limit.isEmpty || _limit.exists(cost.matched < _))) {
      cost ++= processIteration(rowID, condition)(includeRow)(process)
      rowID += 1
    }
    cost
  }

  def partitionBy(columnNames: Array[String]): RowCollection = {
    val columnName = columnNames.headOption || die("No columns specified")
    val partitionColumnIndex = columns.indexWhere(_.name == columnName)
    assert(partitionColumnIndex != -1, dieNoSuchColumn(columnName))
    val rc = PartitionedRowCollection(ns, columns, partitionColumnIndex)
    rc.insert(this)
    rc
  }

  def product(that: RowCollection): RowCollection = {
    val productColumns = columns.toList ::: that.columns.toList
    val rc = createQueryResultTable(productColumns)
    var rowID: ROWID = 0L
    this.foreach { rowA =>
      that.foreach { rowB =>
        rc.insert(Row(rowID, rowA.metadata, productColumns, fields = rowA.fields.toList ::: rowB.fields.toList))
        rowID += 1
      }
    }
    rc
  }

  /**
   * Processes a single iteration
   * @param rowID      the ID of the row being processed
   * @param condition  the [[Condition condition]]
   * @param includeRow the function that determines via metadata whether a row will be processed
   * @param process    the function to process selected rows
   * @param scope      the implicit [[Scope scope]]
   * @return
   */
  protected def processIteration(rowID: ROWID, condition: Option[Condition])
                                (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    var cost: IOCost = IOCost()
    if (includeRow(readRowMetadata(rowID))) {
      val rowScope = getRowScope(rowID)
      cost = cost.copy(scanned = cost.scanned + 1)
      rowScope.getCurrentRow foreach { row =>
        if (condition.isEmpty || condition.exists(RuntimeCondition.isTrue(_)(rowScope))) {
          process(rowScope, row).unwrapOptions match {
            case cc: IOCost => cost ++= cc
            case _ =>
          }
          cost = cost.copy(matched = cost.matched + 1)
        }
      }
    }
    cost
  }

  override def pop(): Row = super.pop()

  override def push(item: Any): IOCost = item.normalize match {
    case pc: ProductCollection[_] => pc.host.push(item)
    case row: Row => insert(row)
    case rc: RowCollection => insert(rc)
    case m: QMap[_, _] => push(m.map { case (k, v) => String.valueOf(k) -> v }.toRow(this))
    case x => dieIllegalType(x)
  }

  /**
   * Reads a data block via a pointer from a BLOB store
   * @param ptr the [[Pointer pointer]]
   * @return the corresponding [[IBLOB BLOB]]
   * @example {{{
   *   val passengers = ns('test.serverless.passengers')
   *   passengers.readBLOB(pointer(2048, 1024, 69))
   * }}}
   */
  def readBLOB(ptr: Pointer): IBLOB = die("Embedded BLOBs are not supported by this device")

  def remove(rowID: ROWID): Boolean = {
    val rmd = readRowMetadata(rowID)
    if (rmd.isActive) updateRowMetadata(rowID, rmd.copy(isAllocated = false))
    rmd.isActive
  }

  override def returnType: TableType = TableType(columns)

  def reverse: RowCollection = {
    val out = createQueryResultTable(columns)
    var cost = IOCost()
    for {
      p0 <- 0L to getLength / 2
      p1 = (getLength - p0) - 1
      (r0, r1) = (apply(p0), apply(p1))
    } cost ++= out.update(p0, r1) ++ out.update(p1, r0)
    out
  }

  def search(condition: Option[Condition], limit: Option[Expression] = None)(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    select(AllFields).where(condition).limit(limit).execute()
  }

  def show(): Unit = show(20)

  def show(limit: Int): Unit = {
    tabulate(limit).foreach(System.out.println)
  }

  def shuffle(): IOCost = {
    val rng = new Random()
    val eof = getLength
    var cost = reverseInPlace()
    var rowIDA = 0L
    while (rowIDA < eof) {
      val rowIDB = rng.nextLong(eof)
      if (rowIDA != rowIDB) cost ++= swap(rowIDA, rowIDB)
      rowIDA += 1
    }
    while (rowIDA >= 0) {
      val rowIDB = rng.nextLong(eof)
      if (rowIDA != rowIDB) cost ++= swap(rowIDA, rowIDB)
      rowIDA -= 1
    }
    cost
  }

  override def slice(rowID0: ROWID, rowID1: ROWID): RowCollection = {
    val out = createQueryResultTable(columns)
    for {
      rowID <- rowID0 to rowID1
      row <- get(rowID)
    } out.insert(row)
    out
  }

  /**
   * Performs an in-place sorting of the collection
   * @param get         the row ID to value extraction function
   * @param isAscending indicates whether the sort order is ascending
   */
  def sortInPlace[B](get: ROWID => Option[B], isAscending: Boolean = true): IOCost = {
    val cache = mutable.Map[ROWID, Option[B]]()
    var cost = IOCost()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, {
      cost ++= IOCost(scanned = 1)
      get(rowID)
    })

    def isLesser(n: ROWID, high: ROWID): Boolean = if (isAscending) fetch(n) < fetch(high) else fetch(n) > fetch(high)

    def partition(low: ROWID, high: ROWID): ROWID = {
      var m = low - 1 // index of lesser item
      for (n <- low until high) if (isLesser(n, high)) {
        m += 1 // increment the index of lesser item
        _swap(m, n)
      }
      _swap(m + 1, high)
      m + 1
    }

    def sort(low: ROWID, high: ROWID): Unit = if (low < high) {
      val pi = partition(low, high)
      sort(low, pi - 1)
      sort(pi + 1, high)
    }

    def _swap(offset0: ROWID, offset1: ROWID): Unit = {
      val (elem0, elem1) = (cache.remove(offset0), cache.remove(offset1))
      elem0.foreach(v => cache(offset1) = v)
      elem1.foreach(v => cache(offset0) = v)
      cost ++= swap(offset0, offset1)
    }

    sort(low = 0, high = getLength - 1)
    cost
  }

  def subtract(that: RowCollection): RowCollection = {
    verifyColumnsMatch(that)
    var rowID: ROWID = 0
    val out = createQueryResultTable(columns)
    val set = mutable.HashSet[Seq[Field]]()
    val (device0, device1) = (this, that)
    device1 foreach { row => set += row.fields }
    device0 foreach { row =>
      if (!set.contains(row.fields)) {
        out.update(rowID, row)
        rowID += 1
      }
    }
    out
  }

  def tail: RowCollection = {
    val out = createTempTable(this)
    for {start <- indexWhereMetadata()(_.isActive)} {
      var id: ROWID = start + 1
      val eof = getLength
      while (id < eof) {
        val row = apply(id)
        if (row.metadata.isActive) out.insert(row)
        id += 1
      }
    }
    out
  }

  override def toMap(row: Row): Map[String, Any] = row.toMap

  override def toString: String = s"${getClass.getSimpleName}(${columns.map(_.name).mkString(", ")})"

  override def toTable(implicit scope: Scope): RowCollection = this

  override def toTableType: TableType = TableType(columns)

  /**
   * Restores rows within an inner-table matching the given criteria (up to the optionally specified limit)
   * @param innerTableColumn the [[TableColumn column]] that contains the inner-table
   * @param condition        the deletion [[Condition criterion]]
   * @param limit            the maximum number of records to delete
   * @return the number of rows restored
   * @example {{{
   *  undelete from stocks#transactions
   *  where symbol is 'SHMN'
   *  and wherein transactions (price is 0.001)
   * }}}
   */
  def undeleteInside(innerTableColumn: TableColumn,
                     condition: Option[Condition],
                     limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    // rewrite the condition without 'WhereIn' clause
    val outerCondition = condition.flatMap(_.rewrite {
      case WhereIn(f, _) if f.getNameOrDie == innerTableColumn.name => None
    })
    // search for the extant row, and restore the extinct row
    iterateWhere(outerCondition, limit)(_.isActive) { (outerRowScope, outerRow) =>
      outerRow.getField(innerTableColumn.name).flatMap(_.value) collect {
        case innerTable: RowCollection =>
          val innerCondition = getInnerTableCondition(innerTableColumn, condition)
          val out = getDurableInnerTable(outerTable = this, innerTable, innerTableColumn, outerRow)
          out.iterateWhere(innerCondition)(_.isDeleted) { case (_, innerRow) => out.undelete(innerRow.id) }(outerRowScope)
      }
    }
  }

  /**
   * Restores rows matching the given criteria (up to the optionally specified limit)
   * @param condition the deletion criteria
   * @param limit     the maximum number of records to delete
   * @return the number of rows restores
   */
  def undeleteWhere(condition: Option[Condition], limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    iterateWhere(condition, limit)(_.isDeleted) { case (_, row) => undelete(row.id) }
  }

  /**
   * Modifies rows within an inner-table matching a conditional expression
   * @param innerTableColumn the [[TableColumn column]] that contains the inner-table
   * @param modification     the [[ScopeModification modification]]
   * @param condition        the update [[Condition criterion]]
   * @param limit            the maximum number of records to update
   * @param scope            the implicit [[Scope scope]]
   * @return the [[IOCost cost]]
   * @example
   * {{{
   * update stocks#transactions
   * set price = 103.45
   * where symbol is 'GENS'
   * and wherein transactions (transactionTime is "2021-08-05T19:23:12.000Z")
   * limit 20
   * }}}
   */
  def updateInside(innerTableColumn: TableColumn,
                   modification: ScopeModification,
                   condition: Option[Condition],
                   limit: Option[Expression] = None)(implicit scope: Scope): IOCost = {
    iterateWhere(condition, limit)(_.isActive) { (outerRowScope, outerRow) =>
      outerRow.getField(innerTableColumn.name).flatMap(_.value) collect { case innerTable: RowCollection =>
        val innerCondition = getInnerTableCondition(innerTableColumn, condition)
        val out = getDurableInnerTable(outerTable = this, innerTable, innerTableColumn, outerRow)
        out.iterateWhere(innerCondition)(_.isActive) { (_, innerRow) => out.updateRow(innerRow.id, modification, innerRow) }(outerRowScope)
      }
    }
  }

  /**
   * Updates a row by ID in this device
   * @param rowID        the ID of the row to update
   * @param modification the [[ScopeModification modification]]
   * @param row          the [[Row row]]
   * @param scope        the implicit [[Scope scope]]
   * @return the [[IOCost cost]]
   * @example
   * {{{
   * update stocks
   * set name = 'Apple, Inc.', sector = 'Technology', industry = 'Computers', lastSale = 203.45
   * where symbol is 'AAPL'
   * limit 20
   * }}}
   */
  def updateRow(rowID: ROWID, modification: ScopeModification, row: Row)(implicit scope: Scope): IOCost = {
    val (scope1, cost1, _) = modification.execute(scope)
    val updatedRow = Map(columns.map(_.name) flatMap { name =>
      (scope1.resolve(name) ?? row.getField(name).flatMap(_.value)).map(name -> _)
    }: _*)
    cost1 ++ update(row.id, updatedRow.toRow(this))
  }

  def updateWhere(modification: ScopeModification,
                  condition: Option[Condition],
                  limit: Option[Expression] = None)(implicit scope0: Scope): IOCost = {
    iterateWhere(condition, limit)(_.isActive) { case (scope, row) =>
      val (scope1, cost1, _) = modification.execute(scope)
      val updatedRow = Map(columns.map(_.name) flatMap {
        case name if scope1.getValueReferences.contains(name) =>
          for {valueRef <- scope1.getValueReferences.get(name); value <- Option(valueRef.value)} yield name -> value
        case name =>
          scope1.getCurrentRow.flatMap(_.get(name)).map(name -> _)
      }: _*)
      update(row.id, updatedRow.toRow(this)) ++ cost1
    }
  }

  /**
   * Combines the contents of two query sources
   * @param that the [[RowCollection device]] to combine this device to
   * @return a [[RowCollection device]] containing the contents of the two query sources
   */
  def union(that: RowCollection): RowCollection = {
    verifyColumnsMatch(that)
    var rowID: ROWID = 0
    val out = createQueryResultTable(columns)
    Seq(this, that) foreach (_.foreach { row => out.update(rowID, row); rowID += 1 })
    out
  }

  def unionDistinct(that: RowCollection): RowCollection = {
    verifyColumnsMatch(that)
    var rowID: ROWID = 0
    val out = createQueryResultTable(columns)
    val set = mutable.HashSet[Seq[Field]]()
    Seq(this, that) foreach { device =>
      device.foreach { row =>
        if (!set.contains(row.fields)) {
          set += row.fields
          out.update(rowID, row)
          rowID += 1
        }
      }
    }
    out
  }

  def upsert(row: Row, condition: Condition)(implicit scope: Scope): IOCost = {
    var cost = iterateWhere(Option(condition) /*, limit = Some(1.v)*/)(_.isActive) { case (_, r) =>
      update(r.id, row)
    }
    if (cost.updated == 0) {
      val c1 = insert(row)
      cost ++= c1
    }
    cost
  }

  /**
   * Writes a data block to a BLOB store and returns a pointer
   * @param value the data to write
   * @return a new [[Pointer pointer]] to the [[IBLOB BLOB]]
   * @example {{{
   *   val passengers = ns('test.serverless.passengers')
   *   passengers.writeBLOB('Hello World')
   * }}}
   */
  def writeBLOB(value: Any): Pointer = die("Embedded BLOBs are not supported by this device")

  def zipRows(that: RowCollection): RowCollection = {
    var (rowIDA, rowIDB) = (0L, 0L)
    val (lengthA, lengthB, combinedColumns) = (getLength, that.getLength, columns ++ that.columns)
    val out = createQueryResultTable(combinedColumns)
    while (rowIDA < lengthA && rowIDB < lengthB) {
      // find the next active rows for collections A and B
      while (rowIDA < lengthA && readRowMetadata(rowIDA).isDeleted) rowIDA += 1
      while (rowIDB < lengthB && that.readRowMetadata(rowIDB).isDeleted) rowIDB += 1
      // read a row from collections A and B
      val (rowA, rowB) = (apply(rowIDA), that(rowIDB))
      // write the combined row
      out.insert(Row(id = 0, metadata = RowMetadata(), columns = combinedColumns, fields = rowA.fields ++ rowB.fields))
      rowIDA += 1
      rowIDB += 1
    }
    out
  }

  /**
   * Builds a new inner-table oriented condition from the outer-table oriented; eliminating references that are
   * not compatible.
   * @param innerTableColumn the [[TableColumn column]] that contains the inner-table
   * @param condition        the outer-table oriented [[Condition condition]]
   * @return a new [[Condition]]
   */
  private def getInnerTableCondition(innerTableColumn: TableColumn, condition: Option[Condition]): Option[Condition] = {
    condition.flatMap(_ select {
      case WhereIn(f, c) if f.getNameOrDie == innerTableColumn.name => c
    })
  }

  private def verifyColumnsMatch(that: RowCollection): Unit = {
    assert(columns.map(c => c.name -> c.`type`.name) == that.columns.map(c => c.name -> c.`type`.name), "Column mismatch")
  }

}

/**
 * Row Collection
 */
object RowCollection {

  def apply(tableType: TableType): RowCollection = {
    RowCollectionBuilder(columns = tableType.columns).build
  }

  def builder: RowCollectionBuilder = RowCollectionBuilder()

  def dieColumnIndexOutOfRange(columnIndex: Int): Nothing = die(s"Column index is out of range: $columnIndex")

  def dieNotInnerTableColumn(column: TableColumn): Nothing = die(s"${column.name} is not a column containing an inner-table")

  def dieNotSubTable(ns: DatabaseObjectNS): Nothing = die(s"A sub-table reference was expected near '${ns.toSQL}'")

  /**
   * Because inner-tables are typically decoded as [[MemoryRowCollection]]s, which are not durable,
   * this method substitutes the device with one that is.
   * @param outerTable       the [[RowCollection outer-table]]
   * @param innerTable       the [[RowCollection inner-table]]
   * @param innerTableColumn the [[TableColumn column]] that contains the inner-table
   * @param outerRow         the outer-table [[Row row]]
   * @return a persistable [[RowCollection device]]
   */
  def getDurableInnerTable(outerTable: RowCollection,
                           innerTable: RowCollection,
                           innerTableColumn: TableColumn,
                           outerRow: Row): RowCollection = {
    innerTable match {
      case inMemoryInnerTable: RowCollection if inMemoryInnerTable.isMemoryResident =>
        val columnID = outerTable.columns.indexWhere(_.name == innerTableColumn.name)
        new InnerTableRowCollection(outerTable, inMemoryInnerTable, outerRow.id, columnID)
      case other => other
    }
  }

}