package com.lollypop.runtime.devices

import com.lollypop.runtime.{Boolean2Int, ROWID}
import lollypop.io.{Encodable, IOCost}

import scala.annotation.tailrec

/**
 * Represents Record Metadata I/O
 */
trait RecordMetadataIO extends RecordStructure with Encodable {

  /**
   * Closes the underlying file handle
   */
  def close(): Unit

  /**
   * Counts the number of rows whose metadata matches the predicate
   * @param f the function defining which items should be included
   * @return the number of rows matching the predicate
   */
  def countWhereMetadata(f: RowMetadata => Boolean): Long = {
    foldLeftMetadata[Long](0L) { (total, rmd) => total + (if (f(rmd)) 1 else 0) }
  }

  /**
   * Remove an item from the collection via its record offset
   * @param rowID the record offset
   * @return the [[IOCost I/O cost]]
   */
  def delete(rowID: ROWID): IOCost = {
    val rmd = readRowMetadata(rowID)
    if (rmd.isDeleted) IOCost()
    else {
      updateRowMetadata(rowID, rmd.copy(isAllocated = false))
      IOCost(deleted = 1)
    }
  }

  def deleteField(rowID: ROWID, columnID: Int): IOCost = {
    updateFieldMetadata(rowID, columnID, readFieldMetadata(rowID, columnID).copy(isActive = false))
    IOCost(deleted = 1)
  }

  /**
   * @return the internal state of the device as bytes
   */
  def encode: Array[Byte]

  def foldLeftMetadata[A](initialValue: A)(f: (A, RowMetadata) => A): A = {
    var result: A = initialValue
    var rowID: ROWID = 0
    while (hasNext(rowID)) {
      val rmd = readRowMetadata(rowID)
      rowID += 1
      result = f(result, rmd)
    }
    result
  }

  /**
   * @return the number of records in the file, including the deleted ones.
   */
  def getLength: ROWID

  def getRowSummary: RowSummary = {
    foldLeftMetadata[RowSummary](RowSummary()) { (rs, rmd) =>
      rs + RowSummary(
        active = rmd.isActive.toInt,
        deleted = rmd.isDeleted.toInt,
        encrypted = rmd.isEncrypted.toInt,
        replicated = rmd.isReplicated.toInt)
    }
  }

  def hasNext(rowID: ROWID): Boolean = rowID < getLength

  def indexWhereMetadata(initialRowID: ROWID = 0)(f: RowMetadata => Boolean): Option[ROWID] = {
    @tailrec
    def recurse(rowID: ROWID): Option[ROWID] = {
      if (hasNext(rowID)) {
        if (f(readRowMetadata(rowID))) Some(rowID) else recurse(rowID + 1)
      } else None
    }

    recurse(initialRowID)
  }

  def isEmpty: Boolean = !hasNext(0) || lastIndexWhereMetadata()(_.isActive).isEmpty

  def isMemoryResident: Boolean = false

  def lastIndexWhereMetadata(initialRowID: => ROWID = getLength - 1)(f: RowMetadata => Boolean): Option[ROWID] = {

    @tailrec
    def recurse(rowID: ROWID): Option[ROWID] = {
      if (rowID >= 0) {
        if (f(readRowMetadata(rowID))) Some(rowID) else recurse(rowID - 1)
      } else None
    }

    recurse(initialRowID)
  }

  def nonEmpty: Boolean = !isEmpty

  def readField(rowID: ROWID, columnID: Int): Field

  def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata

  def readRowMetadata(rowID: ROWID): RowMetadata

  def setLength(newSize: ROWID): IOCost

  /**
   * @return the physical size (at rest) of this device
   */
  def sizeInBytes: Long

  def undelete(rowID: ROWID): IOCost = updateRowMetadata(rowID, rmd = readRowMetadata(rowID).copy(isAllocated = true))

  def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost

  def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost

  def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost

}