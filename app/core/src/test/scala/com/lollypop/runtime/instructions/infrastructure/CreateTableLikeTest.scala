package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Column, ColumnType, TableModel}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.invocables.InlineCodeBlock
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CreateTableLikeTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateTableLike].getSimpleName) {

    it("should compile: create table like") {
      val stocksRef = DatabaseObjectRef("Stocks")
      val pennyStocksRef = DatabaseObjectRef("PennyStocks")
      val results = compiler.compile(
        s"""|create table $stocksRef (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline String(128), newsDate DateTime )[100]
            |)
            |
            |create table $pennyStocksRef like Stocks ( rating Int )
            |""".stripMargin)
      val columns = List(
        Column("symbol", ColumnType(name = "String", size = 8)),
        Column("exchange", ColumnType.`enum`(enumValues = Seq("AMEX", "NASDAQ", "NYSE", "OTCBB", "OTHEROTC"))),
        Column("lastSale: Double"),
        Column("lastSaleTime: DateTime"),
        Column("headlines", ColumnType.table(capacity = 100, columns = Seq(
          Column("headline", ColumnType(name = "String", size = 128)),
          Column("newsDate DateTime"),
        ))))
      assert(results == InlineCodeBlock(
        CreateTable(stocksRef, TableModel(columns = columns), ifNotExists = false),
        CreateTableLike(pennyStocksRef,
          tableModel = TableModel(columns = List(Column("rating Int"))),
          template = stocksRef,
          ifNotExists = false)
      ))
    }

    it("should decompile create table like") {
      verify(
        s"""|create table PennyStocks like Stocks
            |""".stripMargin)
    }

    it("should decompile create table like with additional columns") {
      verify(
        s"""|create table PennyStocks like Stocks ( rating Int )
            |""".stripMargin)
    }

    it("should execute create table like with additional columns") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.test1'
           |drop if exists Stocks
           |create table Stocks (
           |  symbol: String(8),
           |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |  lastSale: Double,
           |  lastSaleTime: DateTime,
           |  headlines Table ( headline String(128), newsDate DateTime )[100]
           |)
           |
           |drop if exists PennyStocks
           |create table PennyStocks like Stocks ( rating Int )
           |
           |describe ns("PennyStocks")
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("name" -> "rating", "type" -> "Int"),
        Map("name" -> "symbol", "type" -> "String(8)"),
        Map("name" -> "exchange", "type" -> "Enum(AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC)"),
        Map("name" -> "lastSale", "type" -> "Double"),
        Map("name" -> "lastSaleTime", "type" -> "DateTime"),
        Map("name" -> "headlines", "type" -> "Table(headline: String(128), newsDate: DateTime)[100]")
      ))
    }

    it("should execute create table like with overridden columns") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|namespace 'samples.test2'
           |drop if exists Stocks
           |create table Stocks (
           |  symbol: String(8),
           |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |  lastSale: Double,
           |  lastSaleTime: DateTime
           |)
           |
           |drop if exists PennyStocks
           |create table PennyStocks like Stocks ( symbol: String(10), rating Int )
           |
           |describe ns("PennyStocks")
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("name" -> "symbol", "type" -> "String(10)"),
        Map("name" -> "rating", "type" -> "Int"),
        Map("name" -> "exchange", "type" -> "Enum(AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC)"),
        Map("name" -> "lastSale", "type" -> "Double"),
        Map("name" -> "lastSaleTime", "type" -> "DateTime")
      ))
    }

  }

}
