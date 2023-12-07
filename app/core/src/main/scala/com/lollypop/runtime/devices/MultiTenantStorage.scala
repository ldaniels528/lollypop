package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.language._
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.util.LogUtil
import lollypop.io.IOCost
import lollypop.lang.BitArray

import java.nio.ByteBuffer.wrap
import scala.collection.concurrent.TrieMap
import scala.util.{Failure, Try}

/**
 * Multi-Tenant Storage System - supports read, write and delete functionality for multi-tenant tables
 * @param host        the host [[RowCollection collection]]
 * @param blobStorage the [[BlobStorage BLOB storage system]]
 * @param resources   the mappings of column ID to [[RowCollection shared resource]]
 */
class MultiTenantStorage(val host: RowCollection, val blobStorage: BlobStorage, val resources: Map[Int, RowCollection])
  extends DataStorage[RowCollection] {
  private val cache = TrieMap[(ROWID, Int), MultiTenantRowCollection]()

  override def close(): Unit = {
    resources.map(t => t._2.close _)
      .map(f => Try(f())).collect { case Failure(e) => e }
      .foreach { e => LogUtil(this).error(s"Error close device: ${e.getMessage}", e) }
  }

  override def delete(rowID: ROWID, columnID: Int): IOCost = {
    resources.get(columnID) match {
      case Some(resource) =>
        val cost0 = resource.delete(rowID)
        cache.foldLeft(cost0) {
          case (cost, ((_rowID, _), rc)) if _rowID == rowID => rc.visibility.remove(_rowID); cost
          case (cost, _) => cost
        }
      case None => IOCost()
    }
  }

  override def get(rowID: ROWID, columnID: Int): Option[MultiTenantRowCollection] = {
    resources.get(columnID) map { resource =>
      // attempt to pull the multi-tenant collection from cache for a given (rowID, colID)
      cache.getOrElseUpdate(rowID -> columnID, {
        val buf = blobStorage.getOrElse(rowID, columnID, {
          val maxOffset = resource.getLength
          val newBuf = BitArray.withRange(maxOffset, maxOffset + 1).encode.array()
          blobStorage.write(rowID, columnID, newBuf)
          newBuf
        })

        // attach a modification listener to the BitArray
        val visibility = BitArray.decode(wrap(buf))
        visibility.addChangeListener((bitArray: BitArray) => blobStorage.write(rowID, columnID, bitArray.encode.array()))
        MultiTenantRowCollection(resource, visibility)
      })
    }
  }

  def getResource(columnName: String): Option[RowCollection] = {
    host.columns.indexWhere(_.name == columnName) match {
      case -1 => None
      case columnID => resources.get(columnID)
    }
  }

  def occupies(columnID: Int): Boolean = resources.contains(columnID)

  override def read(rowID: ROWID, columnID: Int): MultiTenantRowCollection = {
    get(rowID, columnID) || die(s"No collection found for column # $columnID")
  }

  override def sizeInBytes: Long = resources.values.map(_.sizeInBytes).sum

  override def write(rowID: ROWID, columnID: Int, rows: RowCollection): IOCost = {
    val source = resources(columnID)
    val target = read(rowID, columnID)
    target.visibility.takeAll().foreach(source.delete)
    target.insert(rows)
  }

}

object MultiTenantStorage {

  /**
   * Creates a new multi-tenant row storage system
   * @param ns the [[DatabaseObjectNS object namespace]]
   * @return a new [[MultiTenantStorage multi-tenant storage system]]
   */
  def apply(ns: DatabaseObjectNS): MultiTenantStorage = apply(ns, BlobStorage(ns))

  /**
   * Creates a new multi-tenant row storage system
   * @param ns          the [[DatabaseObjectNS object namespace]]
   * @param blobStorage the [[BlobStorage BLOB storage system]]
   * @return a new [[MultiTenantStorage multi-tenant storage system]]
   */
  def apply(ns: DatabaseObjectNS, blobStorage: BlobStorage): MultiTenantStorage = {
    val config = ns.getConfig
    val host = FileRowCollection(file = ns.tableDataFile, columns = config.columns)
    new MultiTenantStorage(host, blobStorage, resources =
      if (!isMultiTenantStorage(config)) Map.empty else {
        Map(config.columns.map(_.`type`).zipWithIndex.collect { case (tableType: TableType, index) =>
          index -> FileRowCollection(tableType.columns, ns.getMultiTenantDataFile(index))
        }: _*)
      }
    )
  }

  /**
   * Creates a new multi-tenant row storage system
   * @param ref         the [[DatabaseObjectRef object reference]]
   * @param blobStorage the [[BlobStorage BLOB storage system]]
   * @param scope       the implicit [[Scope scope]]
   * @return a new [[MultiTenantStorage multi-tenant storage system]]
   */
  def apply(ref: DatabaseObjectRef, blobStorage: BlobStorage)(implicit scope: Scope): MultiTenantStorage = {
    apply(ref.toNS, blobStorage)
  }

  def isMultiTenantStorage(config: DatabaseObjectConfig): Boolean = {
    config.columns.map(_.`type`).exists {
      case t: TableType => t.isMultiTenant
      case _ => false
    }
  }

}