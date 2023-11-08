package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.language.models.{Condition, Expression}
import com.lollypop.runtime.LollypopVM.implicits.InstructionSeqExtensions
import com.lollypop.runtime.devices.PartitionedRowCollection.PartitionedRange
import com.lollypop.runtime.instructions.conditions.RuntimeCondition.RichConditionAtRuntime
import com.lollypop.runtime.instructions.conditions.{EQ, Is}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, ROWID, Scope, safeCast}
import com.lollypop.util.ByteBufferHelper.DataTypeBuffer
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.nio.ByteBuffer
import scala.collection.concurrent.TrieMap

/**
 * Partitioned Row Collection
 * @param ns                   the [[DatabaseObjectRef table reference]]
 * @param columns              the collection of [[TableColumn columns]]
 * @param partitionColumnIndex the partition column index
 * @param build                the function which provides a [[RowCollection row collection]]
 *                             implementation for a given partition key
 */
class PartitionedRowCollection[T](val ns: DatabaseObjectNS,
                                  val columns: Seq[TableColumn],
                                  val partitionColumnIndex: Int,
                                  val build: T => RowCollection)
  extends RowCollection with OptimizedSearch {
  private var partitionList: List[(T, RowCollection)] = Nil
  private val partitionMap: TrieMap[T, RowCollection] = TrieMap()
  private var ranges: List[PartitionedRange[T]] = Nil

  override def apply(rowID: ROWID): Row = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.apply(localRowID).copy(id = rowID)
  }

  override def close(): Unit = partitionMap.values.foreach(_.close())

  override def encode: Array[Byte] = {
    val buf = ByteBuffer.allocate(sizeInBytes.toInt)
    for {
      (_, device) <- partitionList
    } buf.put(device.encode)
    buf.flipMe().array()
  }

  override def getLength: ROWID = partitionMap.values.map(_.getLength).sum

  def getPartitionMap: Map[Any, RowCollection] = partitionMap.toMap

  override def insert(row: Row): IOCost = {
    val partitionKey = row.getField(partitionColumnIndex).value.flatMap(safeCast[T])
    assert(partitionKey.nonEmpty, s"Partition column \"${columns(partitionColumnIndex).name}\" is null")
    val device = partitionKey.map(addOrGetPartition).orNull
    device.insert(row)
  }

  override def iterateWhere(condition: Option[Condition] = None, limit: Option[Expression] = None)
                           (includeRow: RowMetadata => Boolean)(process: (Scope, Row) => Any)(implicit scope: Scope): IOCost = {
    // look for conditions contain the search key
    val partitionColumnName = columns(partitionColumnIndex).name
    val searchKeyExpressions = condition.toList.flatMap(_.collect {
      case EQ(a, b) if a.getAlias.contains(partitionColumnName) => b
      case EQ(a, b) if b.getAlias.contains(partitionColumnName) => a
      case Is(a, b) if a.getAlias.contains(partitionColumnName) => b
      case Is(a, b) if b.getAlias.contains(partitionColumnName) => a
    })

    // evaluate the search keys and convert them to Option[T]
    val (s, c, r) = searchKeyExpressions.transform(scope)
    val searchKeys = r.map(safeCast[T])

    // if EQ condition(s) based on partition key(s) were found ...
    val cost = if (searchKeys.exists(_.nonEmpty)) {
      (for {
        searchKey_? <- searchKeys
        searchKey <- searchKey_?
        device <- partitionMap.get(searchKey)
      } yield device.iterateWhere(condition, limit)(includeRow)(process)).reduce(_ ++ _)
    } else super.iterateWhere(condition, limit)(includeRow)(process)
    cost ++ c
  }

  override def readField(rowID: ROWID, columnID: Int): Field = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.readField(localRowID, columnID)
  }

  override def readFieldMetadata(rowID: ROWID, columnID: Int): FieldMetadata = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.readFieldMetadata(localRowID, columnID)
  }

  override def readRowMetadata(rowID: ROWID): RowMetadata = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.readRowMetadata(localRowID)
  }

  override def setLength(newSize: ROWID): IOCost = {
    assert(newSize == 0, "Partitioned tables cannot be partially truncated")
    partitionMap.values.foreach(_.setLength(newSize))
    IOCost(deleted = 1)
  }

  override def sizeInBytes: Long = partitionMap.values.map(_.sizeInBytes).sum

  override def update(rowID: ROWID, row: Row): IOCost = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.update(localRowID, row)
  }

  override def updateField(rowID: ROWID, columnID: Int, newValue: Option[Any]): IOCost = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.updateField(localRowID, columnID, newValue)
  }

  override def updateFieldMetadata(rowID: ROWID, columnID: Int, fmd: FieldMetadata): IOCost = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.updateFieldMetadata(localRowID, columnID, fmd)
  }

  override def updateRowMetadata(rowID: ROWID, rmd: RowMetadata): IOCost = {
    val (device, localRowID) = getPartitionAndLocalRowID(rowID)
    device.updateRowMetadata(localRowID, rmd)
  }

  private def addOrGetPartition(partitionKey: T): RowCollection = {
    partitionMap.getOrElseUpdate(partitionKey, {
      val partition = build(partitionKey)
      partitionList = partitionList ::: List(partitionKey -> partition)
      ranges = Nil
      partition
    })
  }

  private def getPartitionAndLocalRowID(rowID: ROWID): (RowCollection, ROWID) = {
    // re-generate the ranges on-demand
    if (ranges.isEmpty) {
      ranges = partitionList.foldLeft[List[PartitionedRange[T]]](Nil) {
        case (Nil, (key, partition)) =>
          List(PartitionedRange[T](key, partition, to = 0L, from = partition.getLength - 1))
        case (list, (key, partition)) =>
          list.head match {
            case PartitionedRange(_, _, _, to) => PartitionedRange(key, partition, to + 1, to + partition.getLength) :: list
          }
      }.reverse
    }

    // attempt to locate the device
    ranges.collectFirst {
      case PartitionedRange(_, partition, from, to) if rowID >= from && rowID <= to => (partition, rowID - from)
    } || die(s"$rowID is out of range")
  }

}

object PartitionedRowCollection {

  def apply[T](ns: DatabaseObjectNS, columns: Seq[TableColumn], partitionColumnIndex: Int): PartitionedRowCollection[T] = {
    new PartitionedRowCollection(ns, columns, partitionColumnIndex, build = {
      val builder = RowCollectionBuilder(ns, columns)
      (_: T) => builder.build
    })
  }

  case class PartitionedRange[T](key: T, rc: RowCollection, to: ROWID, from: ROWID)

}
