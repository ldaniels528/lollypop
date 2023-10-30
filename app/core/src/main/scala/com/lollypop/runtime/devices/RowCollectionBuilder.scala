package com.lollypop.runtime.devices

import com.lollypop.die
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.devices.RowCollectionBuilder.{FileSupport, MemorySupport, ShardingSupport}
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef, Scope}

import java.io.File
import scala.reflect.ClassTag

/**
 * Row Collection Builder
 * @param ns              the [[DatabaseObjectNS table namespace]]
 * @param columns         the table/collection columns
 * @param fileSupport     the option of [[FileSupport file support]]
 * @param memorySupport   the option of [[MemorySupport memory support]]
 * @param shardingSupport the option of [[ShardingSupport support]] for distributing data across files
 */
case class RowCollectionBuilder(ns: DatabaseObjectNS = createTempNS(),
                                columns: Seq[TableColumn] = Nil,
                                fileSupport: Option[FileSupport] = None,
                                memorySupport: Option[MemorySupport] = None,
                                shardingSupport: Option[ShardingSupport] = None) {

  /**
   * Creates a new row collection
   * @return a new [[RowCollection row collection]]
   */
  def build: RowCollection = {
    assert(columns.nonEmpty, "No columns specified")
    if (memorySupport.nonEmpty) createMemoryCollection
    else if (shardingSupport.nonEmpty) createShardedCollection
    else createFileCollection
  }

  def withColumns(columns: Seq[TableColumn]): RowCollectionBuilder = this.copy(columns = columns)

  def withMemorySupport(capacity: Int = 0): RowCollectionBuilder = {
    this.copy(memorySupport = Some(MemorySupport(memorySize = capacity)))
  }

  def withNamespace(ns: DatabaseObjectNS): RowCollectionBuilder = this.copy(ns = ns)

  def withPersistence(ref: DatabaseObjectRef)(implicit scope: Scope): RowCollectionBuilder = {
    this.copy(fileSupport = Some(FileSupport(ref)))
  }

  def withPersistence(file: File): RowCollectionBuilder = {
    this.copy(fileSupport = Some(FileSupport(file)))
  }

  def withProduct[A <: Product : ClassTag]: RowCollectionBuilder = {
    val (columns, _) = ProductCollection.toColumns[A]
    this.copy(columns = columns)
  }

  def withSharding(shardSize: Int, builder: RowCollectionBuilder => RowCollectionBuilder = _.copy()): RowCollectionBuilder = {
    this.copy(shardingSupport = Some(ShardingSupport(shardSize, builder(RowCollectionBuilder()).withColumns(columns))))
  }

  def withShardSupport(shardSize: Int, builder: RowCollectionBuilder): RowCollectionBuilder = {
    this.copy(shardingSupport = Some(ShardingSupport(shardSize, builder.withColumns(columns))))
  }

  private def createFileCollection: RowCollection = {
    fileSupport match {
      case Some(FileSupport(file)) => FileRowCollection(columns, file)
      case None => FileRowCollection(columns)
    }
  }

  private def createMemoryCollection: RowCollection = {
    memorySupport match {
      case Some(MemorySupport(memorySize)) =>
        if (memorySize > 0) ByteArrayRowCollection(ns, columns, memorySize) else ModelRowCollection(columns)
      case None => die("Memory support is not configured")
    }
  }

  private def createShardedCollection: ShardedRowCollection = {
    shardingSupport match {
      case Some(ShardingSupport(shardSize, builder)) => ShardedRowCollection(ns, columns, shardSize, shardBuilder = builder)
      case None => die("Sharding support is not configured")
    }
  }

}

object RowCollectionBuilder {

  /**
   * File Support
   * @param persistenceFile the file location for persisting the data
   */
  case class FileSupport(persistenceFile: File = createTempFile())

  object FileSupport {
    def apply(ref: DatabaseObjectRef)(implicit scope: Scope): FileSupport = {
      FileSupport(persistenceFile = ref.toNS.tableDataFile)
    }
  }

  /**
   * Memory Support
   * @param memorySize the optional maximum number of encoded in-memory rows
   */
  case class MemorySupport(memorySize: Int = 0)

  /**
   * Sharding Support
   * @param shardSize    the maximum size of each shard
   * @param shardBuilder the [[RowCollectionBuilder builder]] for creating new [[RowCollection shards]]
   */
  case class ShardingSupport(shardSize: Int, shardBuilder: RowCollectionBuilder = RowCollectionBuilder())

}