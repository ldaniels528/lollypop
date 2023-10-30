package com.lollypop.runtime

import com.lollypop.language.models.{Expression, FunctionCall}
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices._
import com.lollypop.util.StringHelper.StringEnrichment
import org.slf4j.LoggerFactory
import lollypop.lang.OS

import scala.collection.concurrent.TrieMap
import scala.util.Try

/**
 * Resource Manager
 */
object ResourceManager {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val resources = TrieMap[DatabaseObjectNS, Any]()

  def close(ns: DatabaseObjectNS): Unit = {
    resources.remove(ns) foreach {
      case ac: AutoCloseable =>
        // close the resource
        Try(ac.close())

        // delete it if it's a temporary resource
        deleteTemporaryResource(ns)
      case _ =>
    }
  }

  def deleteTemporaryResource(ns: DatabaseObjectNS): Unit = {
    if (ns.toSQL.startsWith("temp.temp.")) {
      logger.info(s"Deleting temporary resource '$ns'...'")
      ns.rootDirectory.deleteRecursively()
    }
  }

  def getResource(ns: DatabaseObjectNS): Option[Any] = resources.get(ns)

  def getResourceOrElseUpdate(ns: DatabaseObjectNS, f: => Any): Any = resources.getOrElseUpdate(ns, {
    logger.info(s"Tracking '$ns'")
    f
  })

  def getResourceMappings: Map[DatabaseObjectNS, Any] = resources.toMap

  def getResources: RowCollection = {
    val out = createQueryResultTable(Seq(
      TableColumn(name = "id", `type` = Int32Type),
      TableColumn(name = "ns", `type` = StringType),
      TableColumn(name = "dataType", `type` = StringType),
      TableColumn(name = "sizeInBytes", `type` = Float64Type)
    ))
    for {
      ((ns, entity), id) <- resources.zipWithIndex
    } {
      out.insert(Map(
        "id" -> id,
        "ns" -> ns.toSQL,
        "sizeInBytes" -> Try(OS.sizeOf(entity)).getOrElse(null),
        "dataType" -> (entity match {
          case fc: FunctionCall => s"Function${fc.args.map(_.toSQL).mkString("(", ", ", ")")}"
          case rc: HashIndexRowCollection => s"Index${rc.hashTable.columns.map(_.name).mkString("(", ", ", ")")}"
          case rc: RowCollection => s"Table${rc.columns.map(_.name).mkString("(", ", ", ")")}"
          case e: Expression => e.toSQL.limit(40)
          case z => Try(Inferences.fromValue(z).name).getOrElse(z.getClass.getSimpleName)
        })
      ).toRow(out))
    }
    out
  }

  def link[T <: DataObject](dataObject: T): T = {
    logger.info(s"Tracking '${dataObject.ns}' [${dataObject.getClass.getSimpleName}]")
    resources.put(dataObject.ns, dataObject)
    dataObject
  }

  def unlink(ns: DatabaseObjectNS): Unit = {
    resources.remove(ns) foreach { _ =>
      deleteTemporaryResource(ns)
    }
  }

  final implicit class RowCollectionTracker[T <: RowCollection](val rc: T) extends AnyVal {
    def track(): T = link(rc)
  }

}
