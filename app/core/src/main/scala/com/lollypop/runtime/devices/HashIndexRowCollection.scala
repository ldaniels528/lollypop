package com.lollypop.runtime.devices

import com.lollypop.language.dieIllegalType
import com.lollypop.language.models.{Condition, Expression, Instruction}
import com.lollypop.runtime.LollypopVM.implicits._
import com.lollypop.runtime.devices.Field.ColumnToFieldExtension
import com.lollypop.runtime.devices.IndexedRowCollection.{BIT_ARRAY_COLUMN_INDEX, BUCKETS, getHashIndexTableColumns}
import com.lollypop.runtime.devices.RowCollection.dieNotSubTable
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.RichConditionAtRuntime
import com.lollypop.runtime.instructions.conditions.{EQ, Is, Isnt, NEQ}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.{DatabaseObjectNS, LollypopVM, ROWID, ResourceManager, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost
import lollypop.lang.BitArray

import scala.math.abs

/**
 * Hash Index Row Collection
 * @param hashTable       the [[RowCollection hash collection]]
 * @param baseTable       the [[RowCollection data collection]]
 * @param indexedColumnID the indexed column ID
 * @param buckets         the maximum number of buckets
 */
class HashIndexRowCollection(override val ns: DatabaseObjectNS,
                             val hashTable: RowCollection,
                             val baseTable: RowCollection,
                             val indexedColumnID: IndexColumnID,
                             val buckets: Int,
                             val isUnique: Boolean)
  extends HostedRowCollection with IndexedRowCollection {
  private val indexedColumnName = columns(indexedColumnID).name

  override def close(): Unit = {
    ResourceManager.unlink(ns)
    hashTable.close()
  }

  override def host: RowCollection = baseTable

  override def insert(row: Row): IOCost = {
    val cost = super.insert(row)
    updateIndexWithRowID(rowID = cost.firstRowID, indexedValue = row.getField(indexedColumnID).value, include = true)
    cost
  }

  override def iterateWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)
                           (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    val args = condition.toList.flatMap(indexCriterion).distinct
    if (args.nonEmpty) {
      val (s, c, r) = LollypopVM.transform(scope, args)
      (for {
        searchValue <- r.map(Option.apply)
        cost = processIndex(searchValue, condition, limit)(includeRow)(process)
      } yield cost).reduce(_ ++ _) ++ c
    } else super.iterateWhere(condition, limit)(includeRow)(process)
  }

  override def rebuild(): IOCost = {
    var cost = hashTable.setLength(0)
    super.foreach { row =>
      cost ++= updateIndexWithRowID(row.id, indexedValue = row.getField(indexedColumnID).value, include = true)
    }
    cost
  }

  def processIndex(searchValue: Option[Any], condition: Option[Condition], limit: Option[Expression])
                  (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    val hashRowID = computeRowID(searchValue)
    var cost = IOCost()
    val _limit = limit.flatMap(_.asInt32)
    hashTable.readField(hashRowID, columnID = BIT_ARRAY_COLUMN_INDEX).value.foreach {
      case bitArray: BitArray =>
        for {
          rowID <- bitArray.ascending if limit.isEmpty | _limit.exists(_ > cost.matched)
        } cost ++= processIteration(rowID, condition)(includeRow)(process)
      case other => dieIllegalType(other)
    }
    cost
  }

  def searchIndex(searchValue: Option[Any]): RowCollection = {
    val out = createQueryResultTable(columns)
    val hashRowID = computeRowID(searchValue)
    hashTable.readField(hashRowID, columnID = BIT_ARRAY_COLUMN_INDEX).value.map {
      case bitArray: BitArray =>
        for {
          rowID <- bitArray.ascending
          row = super.apply(rowID) if row.metadata.isActive
        } yield out.insert(row)
      case other => dieIllegalType(other)
    }
    out
  }

  override def update(rowID: ROWID, row: Row): IOCost = {
    val newValue = row.getField(indexedColumnID).value
    orchestrateRowAndIndexUpdates(rowID, newValue) { () => super.update(rowID, row) }
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    if (columnID == indexedColumnID)
      orchestrateRowAndIndexUpdates(rowID, newValue) { () => super.updateField(rowID, columnID, newValue) }
    else
      super.updateField(rowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    if (columnID == indexedColumnID) {
      val indexedValue = if (fmd.isActive) baseTable.readField(rowID, indexedColumnID).value else None
      orchestrateRowAndIndexUpdates(rowID, indexedValue) { () =>
        updateIndexWithRowID(rowID, indexedValue, include = fmd.isActive)
        super.updateFieldMetadata(rowID, columnID, fmd)
      }
    } else super.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    val cost = super.updateRowMetadata(rowID, rmd)
    if (cost.updated > 0)
      updateIndexWithRowID(rowID, indexedValue = super.readField(rowID, indexedColumnID).value, include = rmd.isActive)
    cost
  }

  def indexCriterion(condition: Condition): List[Instruction] = {
    condition.collect {
      //      case IN(a, b) if a.getAlias.contains(indexedColumnName) => b
      //      case IN(a, b) if b.getAlias.contains(indexedColumnName) => a
      case EQ(a, b) if a.getAlias.contains(indexedColumnName) => b
      case EQ(a, b) if b.getAlias.contains(indexedColumnName) => a
      case Is(a, b) if a.getAlias.contains(indexedColumnName) => b
      case Is(a, b) if b.getAlias.contains(indexedColumnName) => a
      case Isnt(a, b) if a.getAlias.contains(indexedColumnName) => b
      case Isnt(a, b) if b.getAlias.contains(indexedColumnName) => a
      case NEQ(a, b) if a.getAlias.contains(indexedColumnName) => b
      case NEQ(a, b) if b.getAlias.contains(indexedColumnName) => a
    }
  }

  private def computeRowID(value: Option[Any]): ROWID = {
    val cleanValue = value.unwrapOptions
    val hashCode = if (cleanValue != null) cleanValue.hashCode() else 0
    val hashRowID = abs(hashCode) % buckets
    hashRowID
  }

  private def orchestrateRowAndIndexUpdates(rowID: ROWID, newValue: Option[Any])(doUpdate: () => IOCost): IOCost = {
    // lookup the old value
    val oldValue = super.readField(rowID, indexedColumnID).value

    // perform the update
    val cost = doUpdate()

    // modify the hash index?
    val isChanged = oldValue != newValue
    if (cost.updated > 0 & isChanged) {
      updateIndexWithRowID(rowID, oldValue, include = false)
      updateIndexWithRowID(rowID, newValue, include = true)
    }
    cost
  }

  def updateIndexWithRowID(rowID: ROWID, indexedValue: Option[Any], include: Boolean): IOCost = {
    @inline
    def newBitArray = if (include) BitArray(rowID) else BitArray()

    // add/remove the row ID from within the bit array
    val hashRowID = computeRowID(indexedValue)
    val bitArray = hashTable.readField(hashRowID, columnID = BIT_ARRAY_COLUMN_INDEX).value.map {
      case b: BitArray if include => b.add(rowID); b
      case b: BitArray => b.remove(rowID); b
      case x => dieIllegalType(x)
    } ?? Some(newBitArray)

    // update the corresponding index row
    val row = Row(id = hashRowID, RowMetadata(), columns = hashTable.columns, fields = columns.zipWithIndex.map {
      case (column, index) if index == BIT_ARRAY_COLUMN_INDEX => column.withValue(value = Some(bitArray))
      case (column, _) => column.withValue(indexedValue)
    })
    hashTable.update(hashRowID, row)
    IOCost(shuffled = 1)
  }

  def indexKeyExists(searchValue: Option[Any]): Boolean = {
    val hashRowID = computeRowID(searchValue)
    hashTable.readField(hashRowID, columnID = BIT_ARRAY_COLUMN_INDEX).value match {
      case Some(bitArray: BitArray) => bitArray.ascending.exists(rowID => apply(rowID).metadata.isActive)
      case None => false
    }
  }

  def keyOffsets(searchValue: Option[Any]): List[ROWID] = {
    val hashRowID = computeRowID(searchValue)
    hashTable.readField(hashRowID, columnID = BIT_ARRAY_COLUMN_INDEX).value match {
      case Some(bitArray: BitArray) => bitArray.ascending.filter(rowID => apply(rowID).metadata.isActive)
      case None => Nil
    }
  }

  override def toString: String = s"${getClass.getSimpleName}($indexedColumnName)"

}

/**
 * Hash-Index Row Collection Companion
 */
object HashIndexRowCollection {

  def apply(ns: DatabaseObjectNS): HashIndexRowCollection = {
    val cfg = ns.getConfig
    val baseTable = LogicalTableRowCollection(ns)
    val indexedColumnID = ns.columnName.map(baseTable.getColumnIdByNameOrDie) || dieNotSubTable(ns)
    new HashIndexRowCollection(ns,
      hashTable = FileRowCollection(
        columns = getHashIndexTableColumns(baseTable, indexedColumnID),
        file = ns.indexFile || dieNotSubTable(ns)
      ),
      baseTable = baseTable,
      indexedColumnID = indexedColumnID,
      buckets = BUCKETS,
      isUnique = {
        val indexColumnName = baseTable.getColumnByID(indexedColumnID).name
        cfg.indices.exists(idx => idx.indexedColumnName == indexColumnName && idx.isUnique)
      }
    )
  }

}