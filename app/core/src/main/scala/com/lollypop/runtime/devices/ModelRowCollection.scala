package com.lollypop.runtime.devices

import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.EnumType
import com.lollypop.runtime.devices.RowCollection.dieColumnIndexOutOfRange
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import lollypop.io.{IOCost, RowIDRange}

import java.nio.ByteBuffer.allocate
import scala.collection.mutable.ArrayBuffer

/**
 * Represents an in-memory row-model-based database collection
 * @param ns      the [[DatabaseObjectNS object namespace]]
 * @param columns the collection of [[TableColumn table columns]]
 */
class ModelRowCollection(val ns: DatabaseObjectNS, val columns: Seq[TableColumn]) extends MemoryRowCollection {
  private val rows = ArrayBuffer[Row]()

  override def apply(rowID: ROWID): Row = {
    val offset = rowID
    if (hasNext(offset)) rows(offset.toInt) else emptyRow(rowID)
  }

  override def close(): Unit = rows.clear()

  private def emptyRow(id: ROWID): Row = {
    Row(id,
      columns = columns,
      metadata = RowMetadata(isAllocated = false),
      fields = columns.map(c => Field(name = c.name, metadata = FieldMetadata(), value = None)))
  }

  override def encode: Array[Byte] = {
    val buffers = rows.map(row => toRowBuffer(toFieldBuffers(rowID = rows.size, row.fields)))
    val size = buffers.map(_.limit()).sum
    buffers.foldLeft(allocate(size)) { case (agg, buf) => agg.put(buf) }.flipMe().array()
  }

  override def insert(row: Row): IOCost = {
    val rowID = getLength
    rows += reconstructRow(rowID, row)
    IOCost(inserted = 1, rowIDs = RowIDRange(rowID))
  }

  override def getLength: ROWID = rows.size

  override def readField(rowID: ROWID, columnID: Int): Field = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val (column, row) = (columns(columnID), apply(rowID))
    row.getField(column.name) || dieNoSuchColumn(column.name)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = readField(rowID, columnID).metadata

  override def readRowMetadata(rowID: ROWID): RowMetadata = apply(rowID).metadata

  private def reconstructRow(rowID: ROWID, row: Row): Row = {

    def getTypedValue(column: TableColumn, field: Field): Option[Any] = {
      val codec = column.`type`
      val newValue = (field.value ?? column.defaultValue.flatMap(i => Option(i.evaluate()._3)))
        .flatMap(v => Option(codec.convert(v)))
      newValue
    }

    // build a new row with type adjusted values
    row.copy(id = rowID, fields = row.columns zip row.fields map {
      case (column, field) if field.metadata.isActive && column.`type`.isEnum =>
        val enumType = column.`type`.asInstanceOf[EnumType]
        field.copy(value = getTypedValue(column, field).collect { case i: Short => enumType.values(i) })
      case (column, field) if field.metadata.isActive => field.copy(value = getTypedValue(column, field))
      case (_, field) => field.copy(value = None)
    })
  }

  override def setLength(newSize: ROWID): IOCost = {
    val deleted = rows.length - newSize.toInt
    rows.remove(newSize.toInt, rows.length)
    IOCost(deleted = deleted)
  }

  override def sizeInBytes: Long = getLength * recordSize

  override def update(id: ROWID, row: Row): IOCost = {
    val updatedRow = reconstructRow(id, row)
    while (rows.length <= id) rows += emptyRow(rows.length)
    rows(id.toInt) = updatedRow
    IOCost(updated = 1)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    assert(columns.indices isDefinedAt columnID, dieColumnIndexOutOfRange(columnID))
    val row = apply(rowID)
    val updatedRow = row.copy(fields = row.fields.zipWithIndex.map {
      case (f, n) if n == columnID => columns(columnID) ~> { col => f.copy(value = newValue.map(col.`type`.convert)) }
      case (f, _) => f
    })
    update(rowID, updatedRow)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    val row = apply(rowID)
    val updatedRow = row.copy(fields = row.fields.zipWithIndex.map {
      case (f, n) if n == columnID => f.copy(metadata = fmd)
      case (f, _) => f
    })
    update(rowID, updatedRow)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    update(rowID, apply(rowID).copy(metadata = rmd))
  }

}

object ModelRowCollection {

  def apply(columns: Seq[TableColumn]): ModelRowCollection = apply(createTempNS(), columns)

  def apply(ns: DatabaseObjectNS, columns: Seq[TableColumn]) = new ModelRowCollection(ns, columns)

}