package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.language.models.View
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class DeclareViewTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[DeclareView].getSimpleName) {

    it("should support compile declare view (==)") {
      val results = compiler.compile(
        """|declare view if not exists OilAndGas :=
           |select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == DeclareView(ref = "OilAndGas", View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Oil/Gas Transmission"
        )), ifNotExists = true))
    }

    it("should support compile declare view (is)") {
      val results = compiler.compile(
        """|declare view if not exists OilAndGas :=
           |  select Symbol, Name, Sector, Industry, SummaryQuote
           |  from Customers
           |  where Industry is 'Oil/Gas Transmission'
           |""".stripMargin)
      assert(results == DeclareView(ref = "OilAndGas", View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f is "Oil/Gas Transmission"
        )), ifNotExists = true))
    }

    it("should support decompiling declare view") {
      val model = DeclareView(ref = "OilAndGas", View(
        query = Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
          from = DatabaseObjectRef("Customers"),
          where = "Industry".f === "Oil/Gas Transmission"
        )), ifNotExists = true)
      assert(model.toSQL ==
        """declare view if not exists OilAndGas := select Symbol, Name, Sector, Industry, SummaryQuote from Customers where Industry == "Oil/Gas Transmission"""")
    }

    it(s"should query rows from view stocks_view") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|drop if exists stocks_view
            |declare view stocks_view := from (
            |    |---------------------------------------------------------|
            |    | symbol | exchange | lastSale | lastSaleTime             |
            |    |---------------------------------------------------------|
            |    | ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
            |    | ABC    | OTCBB    |   8.1112 | 2022-09-04T09:36:51.007Z |
            |    | BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
            |    | TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
            |    | AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
            |    | BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
            |    | NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
            |    | TRX    | NYSE     |  88.22   | 2022-09-04T09:12:53.009Z |
            |    | TRX    | NYSE     |  88.56   | 2022-09-04T09:12:57.706Z |
            |    | NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
            |    | WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
            |    | ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
            |    | NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
            |    | AAPL   | NASDAQ   | 100.01   | 2022-09-04T09:36:46.033Z |
            |    | AAPL   | NASDAQ   | 100.12   | 2022-09-04T09:36:48.459Z |
            |    | WRKR   | AMEX     | 100.12   | 2022-09-04T09:36:48.459Z |
            |    | BOOTY  | OTCBB    |  13.12   | 2022-09-04T09:51:13.111Z |
            |    |---------------------------------------------------------|
            |)
            |@@stocks_view limit 5
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
        Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z"))
      ))
    }

  }

}
