package com.lollypop.runtime.devices

import com.lollypop.database.QueryResponse
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.{dieIllegalType, dieNoResultSet}
import com.lollypop.runtime.DatabaseManagementSystem.readPhysicalTable
import com.lollypop.runtime.datatypes.Inferences.fromClass
import com.lollypop.runtime.datatypes.{AnyType, Int64Type, StringType, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollection.dieNotInnerTableColumn
import com.lollypop.runtime.plastics.RuntimeClass.implicits.{RuntimeClassConstructorSugar, RuntimeClassProduct}
import com.lollypop.runtime.{DatabaseObjectConfig, DatabaseObjectNS, DatabaseObjectRef, Scope}

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
      new ModelRowCollection(createTempNS(),columns) with CursorSupport
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

  //////////////////////////////////////////////////////////////////////////////////////
  //  IMPLICIT CLASSES
  //////////////////////////////////////////////////////////////////////////////////////

  /**
   * Column TableType Extension
   * @param column the [[TableColumn]]
   */
  final implicit class ColumnTableType(val column: TableColumn) extends AnyVal {
    @inline
    def getTableType: TableType = column.`type` match {
      case tableType: TableType => tableType
      case other => dieIllegalType(other)
    }
  }

  final implicit class MapToTableType[T <: Product](val mapping: QMap[_, _]) extends AnyVal {
    @inline
    def toKeyValueCollection: RowCollection = {
      implicit val out: RowCollection = createQueryResultTable(columns = Seq(
        TableColumn(name = "key", `type` = StringType),
        TableColumn(name = "value", `type` = AnyType)), fixedRowCount = mapping.size)
      mapping.foreach { case (key, value) => out.insert(Map("key" -> key, "value" -> value).toRow) }
      out
    }
  }

  final implicit class ProductClassToTableType[T <: Product](val `class`: Class[T]) extends AnyVal {
    @inline
    def toTableType: TableType = {
      val fields = `class`.getFilteredDeclaredFields
      TableType(columns = fields.map { field => TableColumn(field.getName, `type` = fromClass(field.getType)) })
    }
  }

  final implicit class ProductToRowCollection[T <: Product](val product: T) extends AnyVal {
    def toRowCollection: RowCollection = {
      product match {
        case result: QueryResponse =>
          val out = createQueryResultTable(result.columns, fixedRowCount = result.rows.size)
          for {
            values <- result.rows
            row = Map(result.columns zip values map { case (column, value) => column.name -> value }: _*).toRow(out)
          } out.insert(row)
          out
        case row: Row =>
          val out = createQueryResultTable(row.columns, fixedRowCount = 1)
          out.insert(row)
          out
        case mapping: QMap[_, _] =>
          implicit val out: RowCollection = createQueryResultTable(columns = Seq(
            TableColumn(name = "key", `type` = StringType),
            TableColumn(name = "value", `type` = AnyType)), fixedRowCount = mapping.size)
          mapping.foreach { case (key, value) => out.insert(Map("key" -> key, "value" -> value).toRow) }
          out
        case _ =>
          val fields = product.productElementFields
          val columns = fields.map { field =>
            TableColumn(name = field.getName, `type` = fromClass(field.getType))
          }
          val out = createQueryResultTable(columns, fixedRowCount = 1)
          out.insert(Map(fields.map(_.getName) zip product.productIterator map {
            case (name, value) => name -> value
          }: _*).toRow(out))
          out
      }
    }

    @inline
    def toTableType: TableType = product.getClass.toTableType
  }

  final implicit class RowsToRowCollection(val rows: Seq[Row]) extends AnyVal {
    def toRowCollection: RowCollection = {
      rows.headOption map { firstRow =>
        val out = createQueryResultTable(firstRow.columns, fixedRowCount = rows.length)
        out.insert(firstRow)
        rows.tail.foreach(out.insert)
        out
      } getOrElse dieNoResultSet()
    }
  }

  /**
   * Rich DatabaseObjectRef
   * @param ref the [[DatabaseObjectRef]]
   */
  final implicit class RichDatabaseObjectRef(val ref: DatabaseObjectRef) extends AnyVal {

    /**
     * Provides a function to process an inner-table
     * @param action the call-back function, which provides a tuple containing the [[RowCollection outer-table]]
     *               and the [[TableColumn column]] (within the outer-table) that contains the inner-table.
     * @param scope  the implicit [[Scope scope]]
     * @tparam A the generic return type
     * @return the return value
     */
    def inside[A](action: (RowCollection, TableColumn) => A)(implicit scope: Scope): A = {
      ref match {
        // is it an inner table reference?
        case DatabaseObjectRef.SubTable(outerTableRef, innerTableColumnName) =>
          // get the outer-table, inner-table and outer-table column containing the inner-table
          val outerTable = scope.getRowCollection(outerTableRef)
          val innerTableColumn = outerTable.getColumnByName(innerTableColumnName) match {
            case column if column.`type`.isTable => column
            case column => dieNotInnerTableColumn(column)
          }
          // perform the action
          action(outerTable, innerTableColumn)
        case other => other.dieNotInnerTable()
      }
    }
  }

}
