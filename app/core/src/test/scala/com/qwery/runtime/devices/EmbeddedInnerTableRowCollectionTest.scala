package com.qwery.runtime.devices

import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.runtime.devices.errors.EmbeddedWriteOverflowError
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseManagementSystem, DatabaseObjectRef, QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class EmbeddedInnerTableRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[EmbeddedInnerTableRowCollection].getSimpleName) {

    it("should allow read/write access to a clustered inner-table via (rowID, columnID)") {
      val ref = DatabaseObjectRef(getTestTableName)
      implicit val (scope, _, _) = QweryVM.executeSQL(Scope(),
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
            |values ('AAPL', 'NASDAQ', {price:156.39, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('AMD', 'NASDAQ',  {price:56.87, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('INTC','NYSE',    {price:89.44, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {price:988.12, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{price:0.0010, transactionTime:"2021-08-05T19:23:11.000Z"},
            |                          {price:0.0011, transactionTime:"2021-08-05T19:23:12.000Z"}])
            |""".stripMargin)
      // get the outer table
      val device = scope.getRowCollection(ref)
      device.tabulate().foreach(logger.info)

      // get a reference to the inner table and add some rows
      val embedded = EmbeddedInnerTableRowCollection(ref, rowID = device.getLength - 1, columnID = device.getColumnIdByNameOrDie("transactions"))
      embedded.insert(Map("price" -> 0.0012, "transactionTime" -> "2021-08-05T19:23:13.000Z").toRow(embedded))
      embedded.insert(Map("price" -> 0.0011, "transactionTime" -> "2021-08-05T19:23:15.000Z").toRow(embedded))
      embedded.insert(Map("price" -> 0.0015, "transactionTime" -> "2021-08-05T19:23:17.000Z").toRow(embedded))

      // attempt to add a record beyond the allocated limit
      assertThrows[EmbeddedWriteOverflowError] {
        embedded.insert(Map("price" -> 0.0017, "transactionTime" -> "2021-08-05T19:23:18.000Z").toRow(embedded))
      }

      // verify the results
      embedded.tabulate().foreach(logger.info)
      assert(embedded.toMapGraph == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
        Map("price" -> 0.0012, "transactionTime" -> DateHelper("2021-08-05T19:23:13.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
        Map("price" -> 0.0015, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
      ))
    }

    it("should perform CRUD against an embedded table in BLOB storage") {
      implicit val scope: Scope = Scope()
      val tableNS = DatabaseObjectRef("temp.embedded.stocks").toNS

      // define the table type
      val tableType = TableType(columns = Seq(
        Column(name = "symbol", `type` = ColumnType("String", size = 6)),
        Column(name = "exchange", `type` = ColumnType("String", size = 6)),
        Column(name = "lastSale", `type` = ColumnType("Float")),
        Column(name = "lastSaleTime", `type` = ColumnType("DateTime"))
      ).map(_.toTableColumn), capacity = 4, isPointer = true)

      // create the table
      DatabaseManagementSystem.dropObject(tableNS, ifExists = true)
      tableNS.createRoot()
      val blobFile = BlobFile(tableNS)
      val ptr = blobFile.allocate(sizeInBytes = tableType.recordSize * 10)
      logger.info(s"BLOB data pointer: $ptr")
      val device = EmbeddedInnerTableRowCollection(blobFile, tableType.columns, ptr)
      device.setLength(newSize = 0)

      // write rows to the device
      val stocks = List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L)))
      stocks.foreach { stock =>
        device.insert(stock.toRow(device))
      }

      logger.info(s"recordSize: ${device.recordSize}, physicalLength: ${device.getLength}, physicalSize: ${device.sizeInBytes}")

      // verify the rows
      device.tabulate() foreach logger.info
      assert(device.tabulate().mkString("\n") ===
        """||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
           || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
           || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
           || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
           ||---------------------------------------------------------|""".stripMargin)
    }

  }

}
