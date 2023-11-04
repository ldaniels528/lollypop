package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{$, @@@, Atom, Column, ColumnType, TableModel}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.invocables.InlineCodeBlock
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DeclareTableLikeTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[DeclareTableLike].getSimpleName) {

    it("should compile: declare table like") {
      val stocksRef = Atom("Stocks")
      val pennyStocksRef = Atom("PennyStocks")
      val results = compiler.compile(
        s"""|declare table ${stocksRef.name} (
            |  symbol: String(8),
            |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
            |  lastSale: Double,
            |  lastSaleTime: DateTime,
            |  headlines Table ( headline String(128), newsDate DateTime )[100]
            |)
            |
            |declare table ${pennyStocksRef.name} like @@Stocks ( rating Int )
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
        DeclareTable(stocksRef, TableModel(columns = columns), ifNotExists = false),
        DeclareTableLike(pennyStocksRef,
          tableModel = TableModel(columns = List(Column("rating Int"))),
          template = @@@(stocksRef.name),
          ifNotExists = false)
      ))
    }

    it("should decompile declare table like") {
      verify(
        s"""|declare table PennyStocks like Stocks
            |""".stripMargin)
    }

    it("should decompile declare table like with additional columns") {
      verify(
        s"""|declare table PennyStocks like Stocks ( rating Int )
            |""".stripMargin)
    }

    it("should execute declare table like with additional columns") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|declare table Stocks (
           |  symbol: String(8),
           |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |  lastSale: Double,
           |  lastSaleTime: DateTime,
           |  headlines Table ( headline String(128), newsDate DateTime )[100]
           |)
           |
           |declare table PennyStocks like Stocks ( rating Int )
           |describe PennyStocks
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

    it("should execute declare table like with overridden columns") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|declare table Stocks (
           |  symbol: String(8),
           |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |  lastSale: Double,
           |  lastSaleTime: DateTime
           |)
           |
           |declare table PennyStocks like Stocks ( symbol: String(10), rating Int )
           |describe PennyStocks
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
