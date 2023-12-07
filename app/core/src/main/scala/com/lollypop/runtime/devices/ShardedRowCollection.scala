package com.lollypop.runtime.devices

import com.lollypop.runtime._
import lollypop.io.IOCost

import java.nio.ByteBuffer.allocate
import scala.collection.mutable.ArrayBuffer

/**
 * Represents a sharded row collection
 * @param ns           the [[DatabaseObjectNS table namespace]]
 * @param columns      the collection of [[TableColumn columns]]
 * @param shardSize    the size of each shard
 * @param shardBuilder the builder responsible for creating new [[RowCollection shards]]
 */
case class ShardedRowCollection(ns: DatabaseObjectNS,
                                columns: Seq[TableColumn],
                                shardSize: Int,
                                shardBuilder: RowCollectionBuilder = RowCollectionBuilder())
  extends RowCollection {
  private val blocks = ArrayBuffer[RowCollection]()
  private val builder = shardBuilder.withColumns(columns)

  override def apply(rowID: ROWID): Row = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.apply(localRowID).copy(id = rowID)
  }

  override def insert(row: Row): IOCost = {
    val (device, localRowID) = getBlockAndLocalRowID(getLength)
    device.update(localRowID, row)
    IOCost(inserted = 1)
  }

  override def close(): Unit = blocks.foreach(_.close())

  override def encode: Array[Byte] = {
    val buffers = blocks.map(_.encode)
    val size = buffers.map(_.length).sum
    (buffers.foldLeft(allocate(INT_BYTES + size).putInt(size)) { case (agg, buf) => agg.put(buf) }).flipMe().array()
  }

  def getBlockAndLocalRowID(rowID: ROWID): (RowCollection, ROWID) = {
    val blockIndex = (rowID / shardSize).toInt
    val localRowID = rowID % shardSize
    (getOrCreateBlockByIndex(blockIndex), localRowID)
  }

  override def getLength: ROWID = blocks.map(_.getLength).sum

  def getOrCreateBlockByIndex(blockIndex: Int): RowCollection = {
    while (blocks.size <= blockIndex) blocks += builder.build
    blocks(blockIndex)
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.readField(localRowID, columnID)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.readFieldMetadata(localRowID, columnID)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.readRowMetadata(localRowID)
  }

  override def setLength(newSize: ROWID): IOCost = {
    // determine the cut-off block index and row ID
    val cutOffIndex = (newSize / shardSize).toInt
    val cutOffRowID = newSize % shardSize

    // adjust the size of the cut-off block
    if (cutOffIndex < blocks.length) blocks(cutOffIndex).setLength(cutOffRowID)

    // truncate the rest
    for {
      block <- (cutOffIndex + 1) until blocks.length map blocks.apply
    } block.setLength(0)
    IOCost(inserted = 1)
  }

  override def sizeInBytes: Long = blocks.map(_.sizeInBytes).sum

  override def update(rowID: ROWID, row: Row): IOCost = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.update(localRowID, row)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.updateField(localRowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.updateFieldMetadata(localRowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    val (device, localRowID) = getBlockAndLocalRowID(rowID)
    device.updateRowMetadata(localRowID, rmd)
  }

}