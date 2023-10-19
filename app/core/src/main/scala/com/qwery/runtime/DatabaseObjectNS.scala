package com.qwery.runtime

import com.qwery.runtime.DatabaseObjectNS._
import com.qwery.runtime.ModelsJsonProtocol._
import com.qwery.runtime.RuntimeFiles.RecursiveFileList
import com.qwery.runtime.devices.{LogicalTableRowCollection, RowCollection}
import com.qwery.runtime.errors.DurableObjectNotFound
import com.qwery.util.JSONSupport.{JSONProductConversion, JSONStringConversion}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper._

import java.io.{File, PrintWriter}
import scala.collection.concurrent.TrieMap
import scala.io.Source

/**
 * Represents a database object namespace
 * @param databaseName the name of the database
 * @param schemaName   the name of the schema
 * @param name         the name of the database object
 * @param columnName   the optional column name of an inner table
 */
case class DatabaseObjectNS(databaseName: String,
                            schemaName: String,
                            name: String,
                            columnName: Option[String] = None)
  extends DatabaseObjectRef {
  private val cachedConfig = new TrieMap[Unit, DatabaseObjectConfig]()

  def rootDirectory: File = getDatabaseRootDirectory(databaseName) / schemaName / name

  def blobDataFile: File = rootDirectory / s"$name.$blobDataExt"

  def clobDataFile: File = rootDirectory / s"$name.$clobDataExt"

  def configFile: File = rootDirectory / s"$name.$configExt"

  def indexFile: Option[File] = columnName.map(col => rootDirectory / s"$name#$col.$indexExt")

  def journalFile: File = rootDirectory / s"$name.$journalFileExt"

  def partitionFile(key: Any): File = rootDirectory / s"$name.${String.valueOf(key).replace(' ', '_')}.$partitionExt"

  def sqlXmlFile: File = rootDirectory / s"$name.$sqlXmlExt"

  def subTableDataFile: Option[File] = columnName.map(col => rootDirectory / s"$name#$col.$dataExt")

  def tableDataFile: File = rootDirectory / s"$name.$dataExt"

  def createRoot(files: File*): Boolean = {
    (rootDirectory.mkdirs() | rootDirectory.exists()) & files.forall(_.createNewFile())
  }

  def getConfig: DatabaseObjectConfig = cachedConfig.getOrElseUpdate((), DatabaseObjectNS.readConfig(this, configFile))

  def getMultiTenantDataFile(columnID: Int): File = rootDirectory / s"$name.tenant.$columnID"

  def getReference: DatabaseObjectRef = {
    val base = DatabaseObjectNS(databaseName, schemaName, name)
    columnName.map(DatabaseObjectRef.InnerTable(base, _)) getOrElse base
  }

  override def isSubTable: Boolean = columnName.nonEmpty

  def readTableAndConfig: (DatabaseObjectConfig, RowCollection) = {
    assert(configFile.exists(), s"Table configuration file for '$this' does not exist [${configFile.getAbsolutePath}]")
    assert(tableDataFile.exists(), s"Table data file for '$this' does not exist [${tableDataFile.getAbsolutePath}]")
    (getConfig, LogicalTableRowCollection(this))
  }

  def writeConfig(config: DatabaseObjectConfig): Unit = {
    DatabaseObjectNS.writeConfig(configFile, config)
  }

  override def toSQL: String = s"$databaseName.$schemaName.$name${columnName.map(col => "#" + col) || ""}"

}

object DatabaseObjectNS {
  val blobDataExt = "blob"
  val clobDataExt = "clob"
  val configExt = "json"
  val dataExt = "table"
  val indexExt = "index"
  val journalFileExt = "journal"
  val partitionExt = "part"
  val sqlXmlExt = "clob"

  def readConfig(ns: DatabaseObjectNS, file: File): DatabaseObjectConfig = {
    if (!file.exists()) throw new DurableObjectNotFound(ns)
    Source.fromFile(file).use(_.mkString.fromJSON[DatabaseObjectConfig])
  }

  def writeConfig(file: File, config: DatabaseObjectConfig): Unit = {
    new PrintWriter(file).use(_.println(config.toJSONPretty))
  }

}