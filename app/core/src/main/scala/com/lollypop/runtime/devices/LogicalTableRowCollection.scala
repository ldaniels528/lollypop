package com.lollypop.runtime.devices

import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.datatypes.{BLOB, IBLOB, Inferences, PointerType}
import com.lollypop.runtime.devices.RowCollectionZoo.ColumnTableType
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, ROWID, Scope}
import com.lollypop.util.LogUtil
import lollypop.io.IOCost
import lollypop.lang.Pointer

import scala.util.{Failure, Try}

/**
 * Represents a logical table with features including variable-length data, embedded tables and BLOBs.
 * @param ns      the [[DatabaseObjectNS object namespace]]
 * @param columns   the complete set of [[TableColumn table columns]]
 * @param clustered the [[FileRowCollection clustered data collection]]
 * @param blob      the [[BlobStorage BLOB storage system]]
 * @param tenant    the [[MultiTenantStorage multi-tenant storage system]]
 */
class LogicalTableRowCollection(val ns: DatabaseObjectNS,
                                val columns: Seq[TableColumn],
                                val clustered: FileRowCollection,
                                val blob: BlobStorage,
                                val tenant: MultiTenantStorage)
  extends RowCollection {
  private val hasClusteredTables = columns.exists(c => !c.isExternal & c.`type`.isTable)

  override def apply(rowID: ROWID): Row = {
    val rowA = if (hasClusteredTables) readIndividually(rowID) else clustered(rowID)
    if (rowA.metadata.isDeleted) rowA
    else rowA.copy(columns = columns, fields = rowA.fields zip columns.zipWithIndex map {
      // is it an inactive field?
      case (field, _) if !field.metadata.isActive => field.copy(value = None)
      // clustered (non-external) field?
      case (field, (column, _)) if !column.isExternal => field
      // multi-tenant row collection?
      case (field, (_, columnID)) if tenant.occupies(columnID) =>
        field.copy(value = Option(tenant.read(rowID, columnID)))
      // BLOB-stored object
      case (field, (column, columnID)) =>
        field.copy(value = Option(blob.readAs(rowID, columnID, column.`type`)))
    })
  }

  override def close(): Unit = {
    Seq(clustered.close _, blob.close _, tenant.close _)
      .map(f => Try(f())).collect { case Failure(e) => e }
      .foreach { e => LogUtil(this).error(s"Error close device: ${e.getMessage}", e) }
  }

  override def delete(rowID: ROWID): IOCost = {
    val cost0 = clustered.delete(rowID)
    if (cost0.deleted == 0) {
      columns.zipWithIndex.foldLeft(cost0) {
        // external object
        case (cost, (column, columnID)) if column.isExternal =>
          cost ++ (if (tenant.occupies(columnID)) tenant.delete(rowID, columnID) else blob.delete(rowID, columnID))
        // clustered object
        case (cost, _) => cost
      }
    } else cost0
  }

  override def encode: Array[Byte] = clustered.encode

  override def getLength: ROWID = clustered.getLength

  override def insert(row: Row): IOCost = {
    // insert a row into the clustered collection
    val cost0 = clustered.insert(createClusteredRow(row))
    // write the external fields
    val rowID = cost0.firstRowID
    val hiddenCost = (row.fields zip columns.zipWithIndex).foldLeft(cost0) {
      // skip clustered (non-external) fields
      case (cost, (_, (column, _))) if !column.isExternal => cost
      // otherwise write the external field
      case (cost, (field, (_, columnID))) =>
        updateField(rowID, columnID, newValue = field.value)
        cost ++ IOCost(updated = 1)
    }
    IOCost(inserted = 1, rowIDs = hiddenCost.rowIDs)
  }

  override def readBLOB(ptr: Pointer): IBLOB = {
    val bytes = blob.entries.read(ptr)
    BLOB.fromBytes(bytes)
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    val column = getColumnByID(columnID)
    if (column.isClustered) {
      // is it a clustered table?
      if (column.`type`.isTable)
        Field(
          name = column.name,
          metadata = readFieldMetadata(rowID, columnID),
          value = Option(EmbeddedInnerTableRowCollection(clustered, rowID, columnID)))
      // clustered field
      else clustered.readField(rowID, columnID)
    }
    // multi-tenant table column?
    else if (tenant.occupies(columnID)) Field(
      name = column.name,
      metadata = readFieldMetadata(rowID, columnID),
      value = Option(tenant.read(rowID, columnID)))
    // BLOB object
    else Field(
      name = column.name,
      metadata = readFieldMetadata(rowID, columnID),
      value = Option(blob.readAs(rowID, columnID, column.`type`)))
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = clustered.readFieldMetadata(rowID, columnID)

  override def readRowMetadata(rowID: ROWID): RowMetadata = clustered.readRowMetadata(rowID)

  override def setLength(newSize: ROWID): IOCost = {
    val cost = clustered.setLength(newSize)
    if (newSize == 0)
      cost ++ tenant.resources.values.foldLeft(IOCost()) { case (aggCost, rc) => aggCost ++ rc.setLength(newSize) }
    else cost
  }

  override def sizeInBytes: Long = clustered.sizeInBytes + blob.sizeInBytes + tenant.sizeInBytes

  override def update(rowID: ROWID, row: Row): IOCost = {
    // insert a row into the clustered collection
    val cost0 = clustered.update(rowID, createClusteredRow(row))
    // finally, write the BLOB fields
    (row.fields zip columns.zipWithIndex).foldLeft(cost0) {
      // skip clustered (non-external) fields
      case (cost, (_, (column, _))) if !column.isExternal => cost
      // otherwise write the external field
      case (cost, (field, (_, columnID))) => cost ++ updateField(rowID, columnID, newValue = field.value)
    }
    IOCost(updated = 1)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    // if not external perform a direct write
    val column = getColumnByID(columnID)
    // clustered (non-external)?
    if (!column.isExternal) clustered.updateField(rowID, columnID, newValue)
    // multi-tenant table column?
    else if (tenant.occupies(columnID)) tenant.write(rowID, columnID, column.getTableType.convert(newValue))
    // BLOB object
    else blob.write(rowID, columnID, column.`type`.encode(newValue))
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    clustered.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = clustered.updateRowMetadata(rowID, rmd)

  override def toString: String = s"${getClass.getSimpleName}($clustered)"

  override def writeBLOB(value: Any): Pointer = {
    val dataType = Inferences.fromValue(value)
    val bytes = dataType.encode(value)
    blob.entries.append(bytes)
  }

  private def createClusteredRow(row: Row): Row = {
    row.copy(columns = clustered.columns, fields = (row.fields zip columns).map {
      case (field, column) if column.isExternal => field.copy(value = None)
      case (field, _) => field
    })
  }

  private def readIndividually(rowID: ROWID): Row = {
    val rmd = readRowMetadata(rowID)
    val _columns = clustered.columns
    if (rmd.isActive) {
      Row(rowID, rmd, _columns, fields = for {columnID <- _columns.indices} yield readField(rowID, columnID))
    } else {
      val fmd = FieldMetadata(isActive = false)
      Row(rowID, rmd, _columns, fields = _columns.map(c => Field(c.name, fmd, value = None)))
    }
  }

}

/**
 * Logical Table Row Collection
 */
object LogicalTableRowCollection {

  /**
   * Creates a new logic table collection
   * @param ns the [[DatabaseObjectNS object namespace]]
   * @return a new [[LogicalTableRowCollection logical table]]
   */
  def apply(ns: DatabaseObjectNS): LogicalTableRowCollection = {
    val config = ns.getConfig
    val clustered = FileRowCollection(file = ns.tableDataFile, columns = config.columns.map {
      case column if column.isExternal => column.copy(`type` = PointerType, defaultValue = None)
      case column => column
    })
    val blobStorage = BlobStorage(ns, clustered)
    new LogicalTableRowCollection(ns,
      columns = config.columns,
      clustered = clustered,
      blob = blobStorage,
      tenant = MultiTenantStorage(ns, blobStorage))
  }

  /**
   * Creates a new logic table collection
   * @param ref   the [[DatabaseObjectRef object reference]]
   * @param scope the implicit [[Scope scope]]
   * @return a new [[LogicalTableRowCollection logical table]]
   */
  def apply(ref: DatabaseObjectRef)(implicit scope: Scope): LogicalTableRowCollection = apply(ref.toNS)

}
