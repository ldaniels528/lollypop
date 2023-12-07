package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.implicits._
import com.lollypop.language.models._
import com.lollypop.language.{Template, _}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.devices.{ByteArrayRowCollection, RowCollection}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.instructions.queryables.RowsOfValues
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CreateTableTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateTable].getSimpleName) {

    it("should use the template to parse statements") {
      val template = Template(CreateTable.templateCard)
      template.tags foreach (t => logger.info(s"|${t.toCode}| ~> $t"))
      val params = template.processWithDebug(
        """|create table if not exists SpecialSecurities (symbol: String, exchange: String, lastSale: Double)
           |partitioned by ['exchange']
           |containing values ('AAPL', 'NASDAQ', 202.11), ('AMD', 'NYSE', 23.50), ('GOOG', 'NYSE', 765.33), ('AMZN', 'NASDAQ', 699.01)
           |""".stripMargin)
      params.all foreach { case (k, v) => logger.info(s"param: $k => $v") }
      assert(params.all == Map(
        "name" -> DatabaseObjectRef("SpecialSecurities"),
        "source" -> RowsOfValues(List(
          List("AAPL", "NASDAQ", 202.11), List("AMD", "NYSE", 23.5),
          List("GOOG", "NYSE", 765.33), List("AMZN", "NASDAQ", 699.01)
        )),
        "columns" -> List("symbol: String".c, "exchange: String".c, "lastSale: Double".c),
        "exists" -> true,
        "keywords" -> Set(")", "create", "containing", "partitioned", "table", "(", "by"),
        "partitions" -> ArrayLiteral("exchange".v)
      ))
    }

    it("should support compiling create table w/ENUM") {
      val results = compiler.compile(
        s"""|create table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime
            |)
            |""".stripMargin)
      val ref = DatabaseObjectRef("Stocks")
      assert(results == CreateTable(ref, TableModel(
        columns = List(
          Column("symbol", "String".ct(size = 8)),
          Column("exchange", ColumnType.`enum`(enumValues = Seq("AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"))),
          Column("lastSale: Double"),
          Column("lastSaleTime: DateTime"))
      ), ifNotExists = false))
    }

    it("should support compiling create table w/ENUM and table") {
      val ref = DatabaseObjectRef("Stocks")
      val results = compiler.compile(
        s"""|create table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline String(128), newsDate DateTime )[100]
            |)
            |""".stripMargin)
      assert(results == CreateTable(ref,
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

    it("should support decompiling create table w/ENUM and inner table") {
      verify(
        s"""|create table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline string(128), newsDate date )[100]
            |)
            |""".stripMargin)
    }

    it("should support decompiling create table w/ENUM") {
      verify(
        s"""|create table Stocks (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale double,
            |  lastSaleTime date
            |)
            |""".stripMargin)
    }

    it("should support executing create table w/an inner-table") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (
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

    it("should support executing create table w/a BLOB inner-table") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (
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

    it("should support executing create table w/a multi-tenant inner-table") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (
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
