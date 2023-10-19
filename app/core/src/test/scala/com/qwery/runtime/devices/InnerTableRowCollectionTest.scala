package com.qwery.runtime.devices

import com.qwery.language.TokenStream
import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RowCollection.getDurableInnerTable
import com.qwery.runtime.devices.RowCollectionZoo.ColumnTableType
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.invocables.SetAnyVariable
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, ROWID, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class InnerTableRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val compiler = QweryCompiler()

  describe(classOf[InnerTableRowCollection].getSimpleName) {

    it("should delete from an inner-table") {
      // setup for a delete from "transactions" (inner-table) on row ID #4
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID) =
        createTable(ref, rowID = 4)

      // verify the inner-table before the delete
      val innerTable0 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable0.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))

      // delete a record from the inner-table
      outerTable.deleteInside(outerTableInnerTableColumn,
        condition = compiler.nextCondition(TokenStream("symbol is 'SHMN' and transactions wherein price is 0.0010")),
        limit = None)(scope)

      // verify the inner-table after the delete
      val innerTable1 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable1.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

    it("should undelete from an inner-table") {
      // setup for a delete from "transactions" (inner-table) on row ID #4
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID) =
        createTable(ref, rowID = 4)

      // verify the inner-table before the delete
      val innerTable0 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable0.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))

      // delete a record from the inner-table
      outerTable.deleteInside(outerTableInnerTableColumn,
        condition = compiler.nextCondition(TokenStream("symbol is 'SHMN' and transactions wherein price is 0.0010")),
        limit = None)(scope)

      // verify the inner-table after the delete
      val innerTable1 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable1.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))

      // undelete the record from the inner-table
      outerTable.undeleteInside(outerTableInnerTableColumn,
        condition = compiler.nextCondition(TokenStream("symbol is 'SHMN' and transactions wherein price is 0.0010")),
        limit = None)(scope)

      // verify the inner-table after the undelete
      val innerTable2 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable2.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

    it("should insert into an inner-table") {
      // setup for an insert into "transactions" (inner-table) on row ID #4
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID) =
        createTable(ref, rowID = 4)

      // verify the inner-table before the insert
      val innerTable0 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable0.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))

      // insert a new record into the inner-table
      outerTable.insertInside(outerTableInnerTableColumn,
        injectColumns = tableType.columns.map(_.name),
        injectValues = Seq(Seq(0.0023.v, "2021-08-05T19:23:13.000Z".v)),
        condition = compiler.nextCondition(TokenStream("symbol is 'SHMN'")),
        limit = None)(scope)

      // verify the inner-table after the insert
      val innerTable1 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable1.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
        Map("price" -> 0.0023, "transactionTime" -> DateHelper("2021-08-05T19:23:13.000Z"))
      ))
    }

    it("should update an inner-table") {
      // setup for an update of "transactions" (inner-table) on row ID #2
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID) =
        createTable(ref, rowID = 2)

      // verify the inner-table before the update
      val innerTable0 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable0.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
      ))

      // update a new record into the inner-table
      outerTable.updateInside(outerTableInnerTableColumn,
        modification = SetAnyVariable("price".f, 90.11.v),
        condition = compiler.nextCondition(TokenStream("symbol is 'INTC'")),
        limit = None)(scope)

      // verify the inner-table after the update
      val innerTable1 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable1.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 90.11, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
      ))
    }

    it("should truncate an inner-table") {
      // setup of truncate against "transactions" (inner-table) on row ID #1
      val ref = DatabaseObjectRef(getTestTableName)
      val (_, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID) =
        createTable(ref, rowID = 1)

      // verify the inner-table before the truncate
      val innerTable0 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable0.toList.flatMap(_.toMapGraph) == List(
        Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
      ))

      // truncate the inner-table
      innerTable0.foreach { innerTable =>
        val outerRow = outerTable.apply(rowID)
        val innerTableWB = getDurableInnerTable(outerTable, innerTable, outerTableInnerTableColumn, outerRow)
        assert(innerTable.isMemoryResident & !innerTableWB.isMemoryResident)
        innerTableWB.setLength(0)
      }

      // verify the inner-table after the truncate
      val innerTable1 = outerTable.readField(rowID, columnID).value.map(tableType.convert)
      assert(innerTable1.toList.flatMap(_.toMapGraph).isEmpty)
    }

  }

  private def createTable(ref: DatabaseObjectRef, rowID: ROWID): (Scope, FileRowCollection, TableColumn, TableType, ROWID, Int) = {
    implicit val (scope0, cost0) = QweryVM.infrastructureSQL(Scope(),
      s"""|drop if exists $ref
          |create table $ref (
          |   symbol: String(8),
          |   exchange: String(8),
          |   transactions Table (
          |       price Double,
          |       transactionTime DateTime
          |   )[5]
          |)
          |
          |insert into $ref (symbol, exchange, transactions)
          |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
          |       ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
          |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
          |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
          |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
          |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
          |""".stripMargin)
    assert(cost0.inserted == 5)

    // get table, column and row details
    val outerTable = FileRowCollection(ref)
    val columnID = outerTable.getColumnIdByNameOrDie(name = "transactions")
    val outerTableInnerTableColumn = outerTable.columns(columnID)
    val tableType = outerTableInnerTableColumn.getTableType
    (scope0, outerTable, outerTableInnerTableColumn, tableType, rowID, columnID)
  }

}
