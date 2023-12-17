package com.lollypop.runtime.devices

import com.lollypop.runtime.DatabaseManagementSystem.readPhysicalTable
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.Int64Type

import java.io.File

/**
 * Row Collection Helper Utility
 */
object RowCollectionZoo {

  //////////////////////////////////////////////////////////////////////////////////////
  //  SPECIALIZED TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  def createMemoryTable(columns: Seq[TableColumn]): RowCollection with CursorSupport = {
    new ModelRowCollection(createTempNS(), columns) with CursorSupport
  }

  def createQueryResultTable(columns: Seq[TableColumn]): RowCollection with CursorSupport = {
    new ModelRowCollection(createTempNS(), columns) with CursorSupport
  }

  def createQueryResultTable(columns: Seq[TableColumn], data: Seq[Map[String, Any]]): RowCollection with CursorSupport = {
    val device = new ModelRowCollection(createTempNS(), columns) with CursorSupport
    device.insert(data.map(_.toRow(device)))
    device
  }

  def createQueryResultTable(columns: Seq[TableColumn], fixedRowCount: Int): RowCollection with CursorSupport = {
    if (fixedRowCount <= 200)
      new ModelRowCollection(createTempNS(), columns) with CursorSupport
    else
      new FileRowCollection(createTempNS(columns)) with CursorSupport
  }

  def createQueryResultTable(collection: RowCollection): RowCollection = {
    createQueryResultTable(collection.columns)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TEMPORARY TABLES
  //////////////////////////////////////////////////////////////////////////////////////

  private def cloneColumns(columns: Seq[TableColumn]): Seq[TableColumn] = {
    columns map {
      case column if column.`type`.isAutoIncrement => column.copy(`type` = Int64Type)
      case column => column
    }
  }

  def createTempTable(columns: Seq[TableColumn]): RowCollection = {
    readPhysicalTable(createTempNS(cloneColumns(columns)))
  }

  def createTempTable(columns: Seq[TableColumn], file: File): RowCollection = {
    FileRowCollection(cloneColumns(columns), file)
  }

  def createTempTable(columns: Seq[TableColumn], fixedRowCount: Int): RowCollection = {
    ByteArrayRowCollection(createTempNS(), cloneColumns(columns), fixedRowCount)
  }

  def createTempTable(columns: Seq[TableColumn], rows: Seq[Row]): RowCollection = {
    implicit val device: RowCollection = createQueryResultTable(cloneColumns(columns))
    rows.foreach(row => device.insert(row.toRow))
    device
  }

  def createTempTable(collection: RowCollection): RowCollection = {
    createTempTable(collection.columns)
  }

  //////////////////////////////////////////////////////////////////////////////////////
  //  TEMPORARY FILES
  //////////////////////////////////////////////////////////////////////////////////////

  def createTempFile(): File = {
    File.createTempFile("lollypop", ".tdb") ~> { file => file.deleteOnExit(); file }
  }

  def createTempNS(): DatabaseObjectNS = {
    DatabaseObjectNS(databaseName = "temp", schemaName = "temp", name = System.nanoTime().toString)
  }

  def createTempNS(columns: Seq[TableColumn]): DatabaseObjectNS = {
    val ns = createTempNS()
    ns.createRoot()
    ns.writeConfig(DatabaseObjectConfig(columns))
    ns
  }

}
