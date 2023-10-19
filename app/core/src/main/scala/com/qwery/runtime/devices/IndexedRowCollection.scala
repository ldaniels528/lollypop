package com.qwery.runtime.devices

import com.qwery.runtime.datatypes.BitArrayType
import qwery.io.IOCost
import qwery.lang.BitArray

/**
 * Represents an Indexed Row Collection
 */
trait IndexedRowCollection extends RowCollection with OptimizedSearch {

  def isUnique: Boolean

  /**
   * Rebuilds the hash index
   * @return the [[IOCost cost]]
   */
  def rebuild(): IOCost

}

/**
 * Indexed Row Collection Companion
 */
object IndexedRowCollection {
  private[devices] val BIT_ARRAY_COLUMN_NAME = "index"
  private[devices] val BIT_ARRAY_COLUMN_INDEX = 1
  private[devices] val BUCKETS = 60000

  def getHashIndexTableColumns(baseTable: RowCollection, indexedColumnID: Int): Seq[TableColumn] = {
    val bitMaxSize = BitArray.encodedSize(BitArray.computeArraySize(maxValue = BUCKETS))
    Seq(baseTable.columns(indexedColumnID), TableColumn(name = BIT_ARRAY_COLUMN_NAME, `type` = BitArrayType(bitMaxSize)))
  }

}