package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class UnionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Union].getSimpleName) {

    it("should decompile select .. union") {
      Seq("all", "distinct", "") foreach { modifier =>
        verify(
          s"""|select Symbol, Name, Sector, Industry, SummaryQuote
              |from Customers
              |where Industry == 'Oil/Gas Transmission'
              |   union $modifier
              |select Symbol, Name, Sector, Industry, SummaryQuote
              |from Customers
              |where Industry == 'Computer Manufacturing'
              |""".stripMargin)
      }
    }

    it("should support select .. union") {
      Seq("all", "") foreach { modifier =>
        val results = compiler.compile(
          s"""|select Symbol, Name, Sector, Industry, SummaryQuote
              |from Customers
              |where Industry == 'Oil/Gas Transmission'
              |   union $modifier
              |select Symbol, Name, Sector, Industry, SummaryQuote
              |from Customers
              |where Industry == 'Computer Manufacturing'
              |""".stripMargin)
        assert(results == Union(
          query0 = Select(
            fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
            from = DatabaseObjectRef("Customers"),
            where = "Industry".f === "Oil/Gas Transmission"),
          query1 = Select(
            fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
            from = DatabaseObjectRef("Customers"),
            where = "Industry".f === "Computer Manufacturing")
        ))
      }
    }

    it("should support select .. union distinct") {
      val results = compiler.compile(
        s"""|select Symbol, Name, Sector, Industry, SummaryQuote
            |from Customers
            |where Industry == 'Oil/Gas Transmission'
            |   union distinct
            |select Symbol, Name, Sector, Industry, SummaryQuote
            |from Customers
            |where Industry == 'Computer Manufacturing'
            |""".stripMargin)
      assert(results == UnionDistinct(
        query0 = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Oil/Gas Transmission"),
        query1 = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Computer Manufacturing")
      ))
    }

    it("should perform a union between sources") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|namespace 'samples.stocks'
            |drop if exists union_stocks
            |create table union_stocks (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double,
            |   transactions Table (
            |      price Double,
            |      transactionTime DateTime
            |   )[2]
            |)
            |insert into union_stocks (symbol, exchange, lastSale, transactions)
            |values ('AAPL', 'NASDAQ', 156.39, '{"price":156.39, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', 56.87, '{"price":56.87, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('INTC','NYSE', 89.44, '{"price":89.44, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('GMTQ', 'OTCBB', 0.1111, '{"price":0.1111, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', 988.12, '{"price":988.12, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', 0.0011, '[{"price":0.0010, "transactionTime":"2021-08-06T15:23:11.000Z"},
            |                                   {"price":0.0011, "transactionTime":"2021-08-06T15:23:12.000Z"}]')
            |select symbol, exchange from union_stocks where symbol == 'INTC'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AAPL'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AMZN'
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("symbol" -> "INTC", "exchange" -> "NYSE"),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ"),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ")
      ))
    }

    it("should perform a union distinct between sources") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|namespace 'samples.stocks'
            |drop if exists union_stocks
            |create table union_stocks (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double,
            |   transactions Table (
            |      price Double,
            |      transactionTime DateTime
            |   )[2]
            |)
            |insert into union_stocks (symbol, exchange, lastSale, transactions)
            |values ('AAPL', 'NASDAQ', 156.39, '{"price":156.39, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', 56.87, '{"price":56.87, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('INTC','NYSE', 89.44, '{"price":89.44, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('GMTQ', 'OTCBB', 0.1111, '{"price":0.1111, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', 988.12, '{"price":988.12, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', 0.0011, '[{"price":0.0010, "transactionTime":"2021-08-06T15:23:11.000Z"},
            |                                   {"price":0.0011, "transactionTime":"2021-08-06T15:23:12.000Z"}]')
            |select symbol, exchange from union_stocks where symbol == 'INTC'
            |  union distinct
            |select symbol, exchange from union_stocks where symbol == 'INTC'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AAPL'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AAPL'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AMZN'
            |  union
            |select symbol, exchange from union_stocks where symbol == 'AMZN'
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("symbol" -> "INTC", "exchange" -> "NYSE"),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ"),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ")
      ))
    }

  }

}
