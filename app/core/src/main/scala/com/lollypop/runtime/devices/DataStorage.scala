package com.lollypop.runtime.devices

import com.lollypop.runtime.ROWID
import lollypop.io.IOCost

/**
 * Facilitates reading, writing and deleting data within a storage system
 * @tparam T the templated data type
 */
trait DataStorage[T] {

  /**
   * @return the [[RowCollection source]] of the data
   */
  def host: RowCollection

  def close(): Unit

  def delete(rowID: ROWID, columnID: Int): IOCost

  def get(rowID: ROWID, columnID: Int): Option[T]

  def getOrElse(rowID: ROWID, columnID: Int, otherwise: => T): T = get(rowID, columnID).getOrElse(otherwise)

  def read(rowID: ROWID, columnID: Int): T

  def sizeInBytes: Long

  def write(rowID: ROWID, columnID: Int, newValue: T): IOCost

}
