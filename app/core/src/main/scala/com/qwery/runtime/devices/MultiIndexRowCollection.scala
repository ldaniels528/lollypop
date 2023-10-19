package com.qwery.runtime.devices

import com.qwery.die
import com.qwery.language.models.{Condition, Expression, Instruction}
import com.qwery.runtime.devices.IndexedRowCollection.{BUCKETS, getHashIndexTableColumns}
import com.qwery.runtime.devices.errors.{UniqueKeyAlreadyExistsError, UniqueKeyCannotBeDisabledError}
import com.qwery.runtime.{DatabaseObjectNS, QweryVM, ROWID, Scope}
import qwery.io.IOCost

/**
 * Multi-Index Row Collection
 * @param baseTable the [[RowCollection base table]]
 * @param indices   the mappings of column ID to [[HashIndexRowCollection index table]]
 * @param buckets   the maximum number of buckets
 */
class MultiIndexRowCollection(val baseTable: RowCollection,
                              val indices: Map[IndexColumnID, HashIndexRowCollection],
                              val buckets: Int)
  extends HostedRowCollection with IndexedRowCollection {

  override def close(): Unit = indices.values.foreach(_.close())

  override def host: RowCollection = baseTable

  override val isUnique: Boolean = indices.exists(_._2.isUnique)

  override def insert(row: Row): IOCost = {
    ifUnique { indexColumnID => validateInsert(indexColumnID, row) }
    val cost = super.insert(row)
    val newID = cost.firstRowID
    process { indexedColumnID =>
      indices(indexedColumnID).updateIndexWithRowID(newID, indexedValue = row.getField(indexedColumnID).value, include = true)
    }
    cost
  }

  override def iterateWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)
                           (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    val args = condition.toList.flatMap(indexCriterion).distinct
    if (args.nonEmpty) {
      (for {
        (columnID, instruction) <- args
        searchValue = Option(QweryVM.execute(scope, instruction)._3)
      } yield indices(columnID).processIndex(searchValue, condition, limit)(includeRow)(process)) match {
        case Nil => IOCost()
        case list => list.reduce(_ ++ _)
      }
    } else super.iterateWhere(condition, limit)(includeRow)(process)
  }

  override def rebuild(): IOCost = process(indexedColumnID => indices(indexedColumnID).rebuild()).reduce(_ ++ _)

  override def update(rowID: ROWID, row: Row): IOCost = {
    ifUnique { indexColumnID => validateUpdate(indexColumnID, row) }
    val oldRow = super.apply(rowID)
    val cost = super.update(rowID, row)
    if (cost.updated > 0) {
      process { indexedColumnID =>
        // lookup the old value
        val oldValue = oldRow.getField(indexedColumnID).value
        val newValue = row.getField(indexedColumnID).value

        // modify the hash index?
        val isChanged = oldValue != newValue
        if (isChanged) {
          val indexTable = indices(indexedColumnID)
          indexTable.updateIndexWithRowID(rowID, oldValue, include = false)
          indexTable.updateIndexWithRowID(rowID, newValue, include = true)
        }
      }
    }
    cost
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    ifUnique { indexColumnID => validateUpdateField(indexColumnID, rowID, newValue) }
    if (isIndexColumn(columnID)) indices(columnID).updateField(rowID, columnID, newValue)
    else super.updateField(rowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    ifUnique { indexColumnID => validateUpdateFieldMetadata(indexColumnID, columnID, fmd) }
    if (isIndexColumn(columnID)) indices(columnID).updateFieldMetadata(rowID, columnID, fmd)
    else super.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    ifUnique { indexColumnID => validateUpdateRowMetadata(indexColumnID, rowID, rmd) }
    val oldRow = super.apply(rowID)
    val cost = super.updateRowMetadata(rowID, rmd)
    if (cost.updated > 0) {
      process { indexedColumnID =>
        indices(indexedColumnID)
          .updateIndexWithRowID(rowID, indexedValue = oldRow.getField(indexedColumnID).value, include = rmd.isActive)
      }
    }
    cost
  }

  private def indexCriterion(condition: Condition): List[(Int, Instruction)] = {
    process { indexedColumnID => indices(indexedColumnID).indexCriterion(condition).map(indexedColumnID -> _) }.toList.reduce(_ ::: _)
  }

  private def isIndexColumn(columnID: Int): Boolean = indices.contains(columnID)

  private def ifUnique(f: IndexColumnID => Unit): Unit = {
    indices.foreach {
      case (indexColumnID, rc) if rc.isUnique => f(indexColumnID)
      case _ =>
    }
  }

  private def process[A](f: Int => A): Seq[A] = {
    for {
      indexedColumnID <- columns.indices if indices.contains(indexedColumnID)
    } yield f(indexedColumnID)
  }

  private def validateInsert(indexColumnID: IndexColumnID, row: Row): Unit = {
    for {
      rc <- indices.get(indexColumnID)
      indexColumnName = rc.getColumnByID(indexColumnID).name
    } {
      // the key must not already exists
      val newValue = row.get(indexColumnName)
      if (rc.indexKeyExists(newValue)) throw new UniqueKeyAlreadyExistsError(indexColumnName, newValue)
    }
  }

  private def validateUpdate(indexColumnID: IndexColumnID, row: Row): Unit = {
    for {
      rc <- indices.get(indexColumnID)
      indexColumnName = rc.getColumnByID(indexColumnID).name
      newValue = row.get(indexColumnName)
    } validateUpdateField(indexColumnID, row.id, newValue)
  }

  private def validateUpdateField(indexColumnID: IndexColumnID, rowID: ROWID, newValue: Option[Any]): Unit = {
    for {
      rc <- indices.get(indexColumnID)
      indexColumnName = rc.getColumnByID(indexColumnID).name
    } {
      // the update must not result in a duplicate key
      val offsets = rc.keyOffsets(newValue)
      if (offsets.size > 1) die(s"Multiple unique values exist for '${newValue.orNull}' for column '$indexColumnName'")
      if (offsets.nonEmpty && !offsets.contains(rowID)) throw new UniqueKeyAlreadyExistsError(indexColumnName, newValue)
    }
  }

  private def validateUpdateFieldMetadata(indexColumnID: IndexColumnID, columnID: Int, fmd: FieldMetadata): Unit = {
    for {
      rc <- indices.get(indexColumnID)
      indexColumnName = rc.getColumnByID(indexColumnID).name
    } {
      // if the field is being disabled, it can't be the unique field
      if (columnID == indexColumnID && fmd.isNull) throw new UniqueKeyCannotBeDisabledError(indexColumnName)
    }
  }

  private def validateUpdateRowMetadata(indexColumnID: IndexColumnID, rowID: ROWID, rmd: RowMetadata): Unit = {
    for {
      rc <- indices.get(indexColumnID)
      indexColumnName = rc.getColumnByID(indexColumnID).name
      row = rc.apply(rowID)
      newValue = row.get(indexColumnName)
    } {
      // if a deleted row is being restores, it update must not result in a duplicate key
      if (row.metadata.isDeleted && rmd.isActive) validateUpdateField(indexColumnID, rowID, newValue)
    }
  }

}

/**
 * Multi-Index Row Collection Companion
 */
object MultiIndexRowCollection {

  def apply(baseTable: RowCollection): MultiIndexRowCollection = {
    new MultiIndexRowCollection(
      baseTable = baseTable,
      indices = getIndices(baseTable.ns, baseTable),
      buckets = BUCKETS)
  }

  def apply(ns: DatabaseObjectNS): MultiIndexRowCollection = {
    apply(baseTable = LogicalTableRowCollection(ns))
  }

  private def getIndices(ns: DatabaseObjectNS, baseTable: RowCollection): Map[IndexColumnID, HashIndexRowCollection] = {
    val config = ns.getConfig
    Map((for {
      indexCfg <- config.indices
      indexedColumnName = indexCfg.indexedColumnName
      indexNS = ns.copy(columnName = Some(indexedColumnName))
      indexFile <- indexNS.indexFile
      indexedColumnID <- baseTable.getColumnIdByName(indexedColumnName)
      columns = getHashIndexTableColumns(baseTable, indexedColumnID)
      hashTable = FileRowCollection(columns, indexFile)
    } yield {
      indexedColumnID -> new HashIndexRowCollection(indexNS, hashTable, baseTable, indexedColumnID, buckets = BUCKETS, isUnique = indexCfg.isUnique)
    }): _*)
  }

}