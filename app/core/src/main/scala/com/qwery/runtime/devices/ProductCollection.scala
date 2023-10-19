package com.qwery.runtime.devices

import com.qwery.language.{ColumnTypeParser, TokenStream, dieNoSuchConstructor}
import com.qwery.runtime.RuntimeClass.implicits.RuntimeClassConstructorSugar
import com.qwery.runtime.datatypes.DataType
import com.qwery.runtime.datatypes.Inferences.fromClass
import com.qwery.runtime.devices.ProductCollection.{KeyValue, toColumns}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.{ColumnInfo, QweryCompiler, ROWID, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

import java.io.File
import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.{implicitConversions, postfixOps}
import scala.reflect.{ClassTag, classTag}

/**
 * Represents a product-based database collection
 * @param host the host [[RowCollection collection]]
 * @tparam T the [[Product product]] type
 */
class ProductCollection[T <: Product : ClassTag](val host: RowCollection) extends RecordCollection[T] {
  override val columns: Seq[TableColumn] = host.columns

  // cache the class information for type T
  private val `class` = toColumns[T]._2
  private val declaredFields = `class`.getFilteredDeclaredFields
  private val declaredFieldNames = declaredFields.map(_.getName)
  private val constructor = (`class`.getConstructors.find(_.getParameterCount == declaredFields.length) ??
    `class`.getConstructors.headOption) || dieNoSuchConstructor(`class`.getName)
  private val parameterTypes = constructor.getParameterTypes

  /**
   * Retrieves the item corresponding to the record offset
   * @param rowID the record offset
   * @return the [[T item]]
   */
  override def apply(rowID: ROWID): T = convertRow(host.apply(rowID))

  /**
   * Computes the average of a column
   * @param predicate the search function
   * @return the [[Double average]]
   */
  def avg(predicate: T => Double): Double = {
    var (count, total) = (0d, 0d)
    foreach { item =>
      total += predicate(item)
      count += 1
    }
    if (count != 0) total / count else Double.NaN
  }

  override def close(): Unit = host.close()

  def collect[U](predicate: PartialFunction[T, U]): List[U] = {
    var out: List[U] = Nil
    foreach(row => out = out ::: (if (predicate.isDefinedAt(row)) Some(predicate(row)) else None).toList)
    out
  }

  def collectFirst[U](predicate: PartialFunction[T, U]): Option[U] = {
    foreach { row => if (predicate.isDefinedAt(row)) return Some(predicate(row)) }
    None
  }

  def contains(elem: T): Boolean = {
    foreach { row => if (elem == row) return true }
    false
  }

  /**
   * Creates an item from a collection of fields
   * @param product the [[T row of data]]
   * @return a new [[T item]]
   */
  private[devices] def convertProduct(product: T): Row = {
    val mapping = Map(declaredFieldNames zip product.productIterator.toSeq map {
      case (name, value: Option[_]) => name -> value
      case (name, value) => name -> Option(value)
    }: _*)
    mapping.toRow(this)
  }

  /**
   * Creates an item from a collection of fields
   * @param row the [[Row row of data]]
   * @return a new [[T item]]
   */
  private[devices] def convertRow(row: Row): T = {
    // get all key-value pairs
    val nameToValueMap: Map[String, Option[Any]] = Map(row.columns zip row.fields collect {
      case (c, f) if c.`type`.isAutoIncrement => f.name -> Some(row.id)
      case (c, f) if f.value.isEmpty => f.name -> c.getTypedDefaultValue
      case (_, f) => f.name -> f.value
    }: _*)
    // extract the raw values
    val rawValues = declaredFieldNames.flatMap(nameToValueMap.get)
    // convert to values for creating the instance
    val normalizedValues = (parameterTypes zip rawValues) map {
      case (param, value) if param == classOf[Option[_]] => value
      case (_, value) => value.map(_.asInstanceOf[AnyRef]).orNull
    }
    constructor.newInstance(normalizedValues: _*).asInstanceOf[T]
  }

  /**
   * Counts the number of items matching the predicate
   * @param predicate the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  def count(predicate: T => Boolean): ROWID = {
    var (rowID: ROWID, matches: ROWID) = (0L, 0L)
    while (hasNext(rowID)) {
      val row_? = get(rowID)
      if (row_?.exists(predicate)) matches += 1
      rowID += 1
    }
    matches
  }

  /**
   * Remove an item from the collection via its record offset
   * @param predicate the search predicate
   * @return the number of records deleted
   */
  def delete(predicate: T => Boolean): IOCost = {
    var stat0 = IOCost()
    val stat1 = _indexOf(_ => false) {
      case (rowID, item) if predicate(item) => stat0 ++= delete(rowID)
      case _ =>
    }
    stat0 ++ stat1
  }

  override def distinct: ProductCollection[T] = {
    val set = mutable.HashSet[T]()
    val out = new ProductCollection[T](createTempTable(columns))
    this.foreach { item =>
      if (!set.contains(item)) {
        set += item
        out.insert(item)
      }
    }
    out
  }

  private def duplicate(copyRows: Boolean, f: T => Boolean = _ => true): ProductCollection[T] = {
    val out = createTempTable(columns)
    if (copyRows) foreach(row => if (f(row)) out.insert(convertProduct(row)))
    new ProductCollection[T](out)
  }

  override def encode: Array[Byte] = host.encode

  def exists(predicate: T => Boolean): Boolean = {
    foreach { row => if (predicate(row)) return true }
    false
  }

  def filter(predicate: T => Boolean): RecordCollection[T] = duplicate(copyRows = true, predicate)

  def filterNot(predicate: T => Boolean): RecordCollection[T] = duplicate(copyRows = true, (v: T) => !predicate(v))

  def find(predicate: T => Boolean): Option[T] = {
    var rowID: ROWID = 0
    while (hasNext(rowID)) {
      val row_? = get(rowID)
      if (row_?.exists(predicate)) return row_?
      rowID += 1
    }
    None
  }

  def forall(predicate: T => Boolean): Boolean = {
    var rowID: ROWID = 0
    while (hasNext(rowID)) {
      val row_? = get(rowID)
      if (!row_?.exists(predicate)) return false
      rowID += 1
    }
    true
  }

  override def get(rowID: ROWID): Option[T] = host.get(rowID).map(convertRow)

  override def getRowScope(rowID: ROWID)(implicit scope: Scope): Scope = host.getRowScope(rowID)

  override def getLength: ROWID = host.getLength

  def headOption: Option[T] = get(rowID = 0)

  def indexOf(elem: T, fromPos: ROWID = 0): ROWID = indexOfOpt(elem, fromPos).getOrElse(-1L: ROWID)

  def indexOfOpt(elem: T, fromPos: ROWID = 0): Option[ROWID] = {
    var index_? : Option[ROWID] = None
    _indexOf(_ => index_?.nonEmpty, fromPos) { (rowID, item) => if (item == elem) index_? = Option(rowID) }
    index_?
  }

  def indexWhere(predicate: T => Boolean): Option[ROWID] = {

    @tailrec
    def recurse(rowID: ROWID): Option[ROWID] = {
      if (hasNext(rowID)) {
        get(rowID) match {
          case Some(row) if predicate(row) => Some(rowID)
          case _ => recurse(rowID + 1)
        }
      } else None
    }

    recurse(rowID = 0)
  }

  /**
   * Appends an item to the end of the file
   * @param record the [[T item]] to append
   * @return [[ProductCollection self]]
   */
  override def insert(record: T): IOCost = host.insert(convertProduct(record))

  def lastOption: Option[T] = get(getLength - 1)

  /**
   * Computes the maximum value of a column
   * @param predicate the search function
   * @return the [[Double maximum value]]
   */
  def max(predicate: T => Double): Double = {
    foldLeft[Double](Double.NaN) { case (agg, row) =>
      val value = predicate(row)
      if (agg.isNaN || agg < value) value else agg
    }
  }

  /**
   * Computes the minimum value of a column
   * @param predicate the search function
   * @return the [[Double minimum value]]
   */
  def min(predicate: T => Double): Double = {
    foldLeft[Double](Double.NaN) { case (agg, row) =>
      val value = predicate(row)
      if (agg.isNaN || agg > value) value else agg
    }
  }

  /**
   * Computes the percentile of a column
   * @param predicate the search function
   * @return the [[Double percentile]]
   */
  def percentile(p: Double)(predicate: T => Double): Double = {
    var sample: List[Double] = Nil
    foreach { item => sample = predicate(item) :: sample }
    if (sample.isEmpty) Double.NaN else {
      val index = Math.round(sample.length * (1.0 - p)).toInt
      sample.sorted.apply(index)
    }
  }

  override def push(item: Any): IOCost = host.push(item)

  override def readField(rowID: ROWID, columnID: Int): Field = host.readField(rowID, columnID)

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    host.readFieldMetadata(rowID, columnID)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = host.readRowMetadata(rowID)

  def reverse: ProductCollection[T] = new ProductCollection[T](host.reverse)

  override def setLength(newSize: ROWID): IOCost = host.setLength(newSize)

  override def sizeInBytes: Long = host.sizeInBytes

  override def slice(rowID0: ROWID, rowID1: ROWID): ProductCollection[T] = {
    new ProductCollection[T](host.slice(rowID0, rowID1))
  }

  /**
   * Performs an in-memory sorting of the collection
   * @param predicate the sort predicate
   */
  def sortBy[B <: Comparable[B]](predicate: T => B): List[T] = toList.sortBy(predicate)

  /**
   * Performs an in-place sorting of the collection
   * @param predicate the sort predicate
   */
  def sortInPlace[B <: Comparable[B]](predicate: T => B): IOCost = {
    val cache = mutable.Map[ROWID, Option[B]]()
    var cost = IOCost()

    def fetch(rowID: ROWID): Option[B] = cache.getOrElseUpdate(rowID, {
      cost ++= IOCost(scanned = 1)
      get(rowID).map(predicate)
    })

    def partition(low: ROWID, high: ROWID): ROWID = {
      var m = low - 1 // index of lesser item
      for {
        pivot <- fetch(high)
        n <- low until high
        value <- fetch(n) if value.compareTo(pivot) < 0
      } {
        m += 1 // increment the index of lesser item
        swap(m, n)
      }
      swap(m + 1, high)
      m + 1
    }

    def sort(low: ROWID, high: ROWID): Unit = if (low < high) {
      val pi = partition(low, high)
      sort(low, pi - 1)
      sort(pi + 1, high)
    }

    def swap(offset0: ROWID, offset1: ROWID): Unit = {
      val (elem0, elem1) = (cache.remove(offset0), cache.remove(offset1))
      elem0.foreach(v => cache(offset1) = v)
      elem1.foreach(v => cache(offset0) = v)
      cost ++= host.swap(offset0, offset1)
    }

    sort(low = 0, high = host.getLength - 1)
    cost
  }

  /**
   * Computes the sum of a column
   * @param predicate the search predicate
   * @return the [[Double sum]]
   */
  def sum(predicate: T => Double): Double = {
    var total: Double = 0
    foreach { item => total += predicate(item) }
    total
  }

  def tail: RecordCollection[T] = BasicIterator(this, isForward = true).skip(count = 1)

  def take(limit: Int): RecordCollection[T] = {
    var (rowID: ROWID, matches: ROWID) = (0L, 0L)
    val out = duplicate(copyRows = false)
    while (hasNext(rowID) && matches < limit) {
      val record_? = get(rowID)
      record_?.foreach { record =>
        out.insert(record)
        matches += 1
      }
      rowID += 1
    }
    out
  }

  private[devices] def toKeyValues(product: T): Seq[KeyValue] = declaredFieldNames zip product.productIterator.toSeq map {
    case (name, value: Option[_]) => name -> value
    case (name, value) => name -> Option(value)
  }

  override def toMap(row: T): Map[String, Any] = Map(toKeyValues(row): _*)

  override def update(rowID: ROWID, row: T): IOCost = host.update(rowID, convertProduct(row))

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    host.updateField(rowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    host.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    host.updateRowMetadata(rowID, rmd)
  }

  def zip[U](that: Iterable[U]): List[(T, U)] = this.toList zip that.toList

}

/**
 * PersistentSeq Companion
 */
object ProductCollection {
  type KeyValue = (String, Option[Any])

  /**
   * Creates a persistent sequence implementation
   * @tparam A the [[Product product type]]
   * @return a new [[ProductCollection persistent sequence]]
   */
  def apply[A <: Product : ClassTag](): ProductCollection[A] = apply[A](RowCollection.builder.withProduct[A])

  /**
   * Creates a persistent sequence implementation
   * @tparam A the [[Product product type]]
   * @param builder the [[RowCollectionBuilder device builder]]
   * @return a new [[ProductCollection persistent sequence]]
   */
  def apply[A <: Product : ClassTag](builder: RowCollectionBuilder): ProductCollection[A] = {
    val (columns, _) = toColumns[A]
    new ProductCollection[A](builder.withColumns(columns).build)
  }

  /**
   * Creates a disk-based sequence implementation
   * @param persistenceFile the persistence [[File file]]
   * @tparam A the [[Product product type]]
   * @return a new [[ProductCollection persistent sequence]]
   */
  def apply[A <: Product : ClassTag](persistenceFile: File): ProductCollection[A] = {
    apply[A](RowCollection.builder.withProduct[A].withPersistence(persistenceFile))
  }

  /**
   * Retrieves the columns that represent the [[Product product type]]
   * @tparam T the [[Product product type]]
   * @return a tuple of collection of [[TableColumn columns]]
   */
  def toColumns[T <: Product : ClassTag]: (List[TableColumn], Class[T]) = {
    val `class` = classTag[T].runtimeClass
    val declaredFields = `class`.getFilteredDeclaredFields
    implicit val scope: Scope = Scope()
    implicit val compiler: QweryCompiler = QweryCompiler(scope.getUniverse)
    val columns = declaredFields map { field =>
      val columnInfo_? = Option(field.getDeclaredAnnotation(classOf[ColumnInfo]))
      val `type` = columnInfo_?
        .collect { case c if c.typeDef().trim.nonEmpty => c.typeDef() }
        .map(typeDef => ColumnTypeParser.nextColumnType(TokenStream(typeDef)))
        .flatMap(columnType => DataType.compile(columnType))
        .getOrElse(fromClass(field.getType))
      TableColumn(name = field.getName, `type` = `type`)
    }
    (columns, `class`.asInstanceOf[Class[T]])
  }

}