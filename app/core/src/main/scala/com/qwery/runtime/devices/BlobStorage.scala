package com.qwery.runtime.devices

import com.qwery.language.diePointerExpected
import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.datatypes._
import com.qwery.runtime.devices.BlobStorage.JournalFile
import com.qwery.runtime.devices.RowCollectionZoo.createTempTable
import com.qwery.runtime.{DatabaseObjectNS, DatabaseObjectRef, ROWID, Scope}
import qwery.io.{Encodable, IOCost}
import qwery.lang.Pointer

import java.io.File
import java.nio.ByteBuffer.wrap

/**
 * Blob Storage System - supports read, write and delete functionality for BLOBs
 * @param host    the host [[RowCollection row collection]]
 * @param entries the [[BlobFile BLOB entries]]; where BLOBs are stored.
 * @param journal the [[JournalFile journal entries]]; where abandoned BLOB allocations are logged.
 */
class BlobStorage(val host: RowCollection, val entries: BlobFile, val journal: JournalFile)
  extends DataStorage[Array[Byte]] {

  override def close(): Unit = {
    entries.close()
    journal.close()
  }

  override def delete(rowID: ROWID, columnID: Int): IOCost = {
    val ptr = readPointer(rowID, columnID)
    journal.log(rowID, columnID, ptr)
    host.deleteField(rowID, columnID)
  }

  override def get(rowID: ROWID, columnID: Int): Option[Array[Byte]] = {
    host.readField(rowID, columnID).value match {
      case Some(ptr: Pointer) => Some(entries.read(ptr))
      case _ => None
    }
  }

  override def read(rowID: ROWID, columnID: Int): Array[Byte] = {
    entries.read(ptr = readPointer(rowID, columnID))
  }

  def readAs(rowID: ROWID, columnID: Int, dataType: DataType): Any = {
    dataType match {
      case tableType: TableType => readCollection(tableType, rowID, columnID)
      case _ => dataType.decode(buf = wrap(read(rowID, columnID)))
    }
  }

  def readCollection(tableType: TableType, rowID: ROWID, columnID: Int): EmbeddedInnerTableRowCollection = {
    EmbeddedInnerTableRowCollection(entries.raf, tableType.columns, ptr = readPointer(rowID, columnID))
  }

  def readCollection(tableType: TableType, ptr: Pointer): EmbeddedInnerTableRowCollection = {
    EmbeddedInnerTableRowCollection(entries.raf, tableType.columns, ptr)
  }

  def readPointer(rowID: ROWID, columnID: Int): Pointer = {
    host.readField(rowID, columnID).value match {
      case Some(ptr: Pointer) => ptr
      case Some(value) => diePointerExpected(value)
      case None => diePointerExpected(None)
    }
  }

  override def sizeInBytes: Long = entries.sizeInBytes + journal.sizeInBytes

  override def write(rowID: ROWID, columnID: Int, bytes: Array[Byte]): IOCost = {

    def insertBlob(): IOCost = host.updateField(rowID, columnID, newValue = Option(entries.append(bytes)))

    // read the field from BLOB storage
    val cost0 = host.readField(rowID, columnID).value match {
      case Some(ptr: Pointer) =>
        // if there's enough space already allocated, just update it.
        if (ptr.allocated >= bytes.length)
          host.updateField(rowID, columnID, newValue = Option(entries.update(ptr, bytes)))
        else {
          // otherwise, insert a new field & track orphaned BLOB allocations
          journal.log(rowID, columnID, ptr)
          insertBlob()
        }
      case Some(value) => diePointerExpected(value)
      case None => insertBlob()
    }
    cost0 ++ IOCost(scanned = 1)
  }

  def write(rowID: ROWID, columnID: Int, encodable: Encodable): IOCost = write(rowID, columnID, encodable.encode)

}

object BlobStorage {

  /**
   * Returns a Blob Storage system for the given namespace
   * @param ns   the [[DatabaseObjectNS object namespace]]
   * @return the [[BlobStorage]]
   */
  def apply(ns: DatabaseObjectNS): BlobStorage = {
    new BlobStorage(FileRowCollection(ns), BlobFile(ns), JournalFile(ns))
  }

  /**
   * Returns a Blob Storage system for the given namespace
   * @param ns   the [[DatabaseObjectNS object namespace]]
   * @param host the [[RowCollection]]
   * @return the [[BlobStorage]]
   */
  def apply(ns: DatabaseObjectNS, host: RowCollection): BlobStorage = {
    new BlobStorage(host, BlobFile(ns), JournalFile(ns))
  }

  /**
   * Returns a Blob Storage system for the given reference
   * @param ref  the [[DatabaseObjectRef object reference]]
   * @param host the [[RowCollection]]
   * @return the [[BlobStorage]]
   */
  def apply(ref: DatabaseObjectRef, host: RowCollection)(implicit scope: Scope): BlobStorage = apply(ref.toNS, host)

  /**
   * Represents a Journal File
   * @param host the [[RowCollection row collection]]
   */
  class JournalFile(val host: RowCollection) {

    def close(): Unit = host.close()

    def entries: RowCollection = ReadOnlyRowCollection(host)

    def log(rowID: ROWID, columnID: Int, ptr: Pointer): IOCost = {
      import JournalFile.{columns, fmd, rmd}
      val row = Row(0, metadata = rmd, columns, fields = Seq(
        Field(name = "rowID", metadata = fmd, value = Some(rowID)),
        Field(name = "columnID", metadata = fmd, value = Some(columnID)),
        Field(name = "ptr", metadata = fmd, value = Some(ptr))
      ))
      host.insert(row)
    }

    def sizeInBytes: Long = host.getLength

  }

  object JournalFile {
    private val rmd = RowMetadata()
    private val fmd = FieldMetadata()
    private val columns = Seq(
      TableColumn(name = "rowID", `type` = Int64Type),
      TableColumn(name = "columID", `type` = Int16Type),
      TableColumn(name = "ptr", `type` = PointerType)
    )

    def apply(file: File): JournalFile = {
      new JournalFile(createTempTable(columns, file))
    }

    def apply(ns: DatabaseObjectNS): JournalFile = {
      new JournalFile(createTempTable(columns, ns.journalFile))
    }
  }

}