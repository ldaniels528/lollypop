package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models._
import com.qwery.runtime.datatypes.{DateTimeType, Float64Type, StringType, TableType}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo.ColumnTableType
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.runtime.devices.{ByteArrayRowCollection, RowCollection, TableColumn}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class DeclareTableTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[DeclareTable].getSimpleName) {

    it("should change the scope") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        s"""|declare table stockQuotes(symbol: String(4), exchange: String(5), lastSale: Double, lastSaleTime: DateTime)
            |""".stripMargin)
      val device_? = scope.resolveAs[RowCollection]("stockQuotes")
      assert(device_?.map(_.columns) contains List(
        TableColumn("symbol", StringType(4)),
        TableColumn("exchange", StringType(5)),
        TableColumn("lastSale", Float64Type),
        TableColumn("lastSaleTime", DateTimeType)
      ))
    }

    it("should support the where ... into command") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|declare table stocks(symbol: String(7), exchange: String(6), lastSale: Double, groupCode: Char)
           | containing values
           |    ('AAXX', 'NYSE', 56.12, 'A'), ('UPEX', 'NYSE', 116.24, 'A'), ('XYZ', 'AMEX',   31.9500, 'A'),
           |    ('JUNK', 'AMEX', 97.61, 'B'), ('RTX.OB',  'OTCBB', 1.93011, 'B'), ('ABC', 'NYSE', 1235.7650, 'B'),
           |    ('UNIB.OB', 'OTCBB',  9.11, 'C'), ('BRT.OB', 'OTCBB', 0.00123, 'C'), ('PLUMB', 'NYSE', 1009.0770, 'C')
           |
           |declare table myStocks (symbol: String(8), exchange: String(8), lastSale: Double, ranking String(8))
           |
           |select symbol, exchange, lastSale: scaleTo(lastSale, 4), ranking: iff(lastSale < 1, 'Penny', 'Standard')
           |from @@stocks
           |where lastSale < 100.0
           |order by symbol
           |into @@myStocks limit 5
           |
           |@@myStocks
           |""".stripMargin
      )
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "ranking" -> "Standard"),
        Map("symbol" -> "BRT.OB", "exchange" -> "OTCBB", "lastSale" -> 0.0012, "ranking" -> "Penny"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61, "ranking" -> "Standard"),
        Map("symbol" -> "RTX.OB", "exchange" -> "OTCBB", "lastSale" -> 1.9301, "ranking" -> "Standard"),
        Map("symbol" -> "UNIB.OB", "exchange" -> "OTCBB", "lastSale" -> 9.11, "ranking" -> "Standard"),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95, "ranking" -> "Standard")
      ))
    }

    it("should support compiling declare table w/ENUM") {
      val results = compiler.compile(
         """|declare table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime
            |)
            |""".stripMargin)
      val ref = Atom("Stocks")
      assert(results == DeclareTable(ref, TableModel(
        columns = List(
          Column("symbol", "String".ct(size = 8)),
          Column("exchange", ColumnType.`enum`(enumValues = Seq("AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"))),
          Column("lastSale: Double"),
          Column("lastSaleTime: DateTime"))
      ), ifNotExists = false))
    }

    it("should support compiling declare table w/ENUM and table") {
      val ref = Atom("Stocks")
      val results = compiler.compile(
         """|declare table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline String(128), newsDate DateTime )[100]
            |)
            |""".stripMargin)
      assert(results == DeclareTable(ref,
        TableModel(columns = List(
          Column("symbol", ColumnType(name = "String", size = 8)),
          Column("exchange", ColumnType.`enum`(enumValues = Seq("AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"))),
          Column("lastSale: Double"),
          Column("lastSaleTime: DateTime"),
          Column("headlines", ColumnType.table(capacity = 100, columns = Seq(
            Column("headline", ColumnType(name = "String", size = 128)),
            Column("newsDate DateTime"),
          ))))
        ), ifNotExists = false))
    }

    it("should support decompiling declare table w/ENUM and inner table") {
      verify(
         """|declare table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline string(128), newsDate date )[100]
            |)
            |""".stripMargin)
    }

    it("should support decompiling declare table w/ENUM") {
      verify(
         """|declare table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale double,
            |  lastSaleTime date
            |)
            |""".stripMargin)
    }

    it("should support executing declare table w/an inner-table") {
      val ref = @@@("stocks")
      val (scope, cost, _) = QweryVM.executeSQL(Scope(),
        """|declare table stocks (
           |   symbol: String(8),
           |   exchange: String(8),
           |   history Table (
           |       price Double,
           |       transactionTime DateTime
           |   )[10]
           |)
           |""".stripMargin)
      assert(cost.created == 1)
      val rc = scope.getRowCollection(ref)
      assert(getStorageType(rc, "history")(_.isClustered))
    }

    it("should support executing declare table w/a BLOB inner-table") {
      val ref = @@@("stocks")
      val (scope, cost, _) = QweryVM.executeSQL(Scope(),
        s"""|declare table ${ref.name} (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (
            |       price Double,
            |       transactionTime DateTime
            |   )[10]*
            |)
            |""".stripMargin)
      assert(cost.created == 1)
      val rc = scope.getRowCollection(ref)
      assert(getStorageType(rc, "transactions")(_.isBlobTable))
    }

    it("should support executing declare table w/a multi-tenant inner-table") {
      val ref = @@@("Stocks")
      val (scope, cost, _) = QweryVM.executeSQL(Scope(),
        s"""|declare table ${ref.name} (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (
            |       price Double,
            |       transactionTime DateTime
            |   )
            |)
            |""".stripMargin)
      assert(cost.created == 1)
      val rc = scope.getRowCollection(ref)
      assert(getStorageType(rc, "transactions")(_.isMultiTenant))
    }

    it("should set default column values upon encode") {
      implicit val scope: Scope = Scope()
      val columns = Seq(
        Column("symbol", ColumnType("String", 8)),
        Column("exchange", ColumnType("String", 8)),
        Column("lastSale", ColumnType("Double")),
        Column("lastSaleTime", ColumnType("Long")),
        Column("description", ColumnType("String", 255)).copy(defaultValue = Some("N/A".v))).map(_.toTableColumn)
      val stocks = List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
      )
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow))
      assert(device.toMapGraph == List(
        Map("description" -> "N/A", "exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("description" -> "N/A", "exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("description" -> "N/A", "exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("description" -> "N/A", "exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

  }

  private def getStorageType(rc: RowCollection, name: String)(f: TableType => Boolean): Boolean = {
    (for {
      _type <- rc.columns.find(_.name == name).map(_.getTableType)
    } yield f(_type)) contains true
  }

}
