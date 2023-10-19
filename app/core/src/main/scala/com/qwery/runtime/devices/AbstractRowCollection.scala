package com.qwery.runtime.devices

import com.qwery.language.models.{Condition, Expression}
import com.qwery.runtime.datatypes.IBLOB
import com.qwery.runtime.{DatabaseObjectNS, ROWID, Scope}
import qwery.io.IOCost
import qwery.lang.Pointer

import scala.annotation.tailrec

/**
 * Abstract Row Collection
 */
trait AbstractRowCollection extends RowCollection {

  /**
   * @return the [[RowCollection source]] of the data
   */
  def out: RowCollection

  override def apply(rowID: ROWID): Row = out.apply(rowID)

  override def ns: DatabaseObjectNS = out.ns

  override def close(): Unit = out.close()

  override def columns: Seq[TableColumn] = out.columns

  override def encode: Array[Byte] = out.encode

  override def getLength: ROWID = out.getLength

  override def insert(record: Row): IOCost = out.insert(record)

  override def isMemoryResident: Boolean = out.isMemoryResident

  override def iterateWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)
                           (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {

    @tailrec
    def recurse(rc: RowCollection): IOCost = rc match {
      case _: OptimizedSearch => out.iterateWhere(condition, limit)(includeRow)(process)
      case w: HostedRowCollection => recurse(w.host)
      case _ => super.iterateWhere(condition, limit)(includeRow)(process)
    }

    recurse(out)
  }

  override def readBLOB(ptr: Pointer): IBLOB = out.readBLOB(ptr)

  override def readField(rowID: ROWID, columnID: Int): Field = out.readField(rowID, columnID)

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = out.readFieldMetadata(rowID, columnID)

  override def readRowMetadata(rowID: ROWID): RowMetadata = out.readRowMetadata(rowID)

  override def setLength(newSize: ROWID): IOCost = out.setLength(newSize)

  override def sizeInBytes: Long = out.sizeInBytes

  override def update(rowID: ROWID, row: Row): IOCost = out.update(rowID, row)

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    out.updateField(rowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    out.updateFieldMetadata(rowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    out.updateRowMetadata(rowID, rmd)
  }

  override def toString: String = s"${getClass.getSimpleName}($out)"

  override def writeBLOB(value: Any): Pointer = out.writeBLOB(value)

}