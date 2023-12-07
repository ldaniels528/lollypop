package com.lollypop.runtime.instructions.queryables

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.aggregation.Unique
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class UniqueTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Unique].getSimpleName) {

    it("should compile select unique") {
      val results = compiler.compile(
        """|select symbol: unique(symbol)
           |from Securities
           |where industry is "Oil/Gas Transmission"
           |""".stripMargin)
      assert(results ==
        Select(
          fields = List(Unique("symbol".f).as("symbol")),
          from = DatabaseObjectRef("Securities"),
          where = "industry".f is "Oil/Gas Transmission"))
    }

    it("should decompile select unique") {
      val model = Select(
        fields = List(Unique("symbol".f).as("symbol")),
        from = DatabaseObjectRef("Securities"),
        where = "industry".f is "Oil/Gas Transmission")
      assert(model.toSQL ==
        """|select symbol: unique(symbol)
           |from Securities
           |where industry is "Oil/Gas Transmission"
           |""".stripMargin.trim.replace("\n", " "))
    }

    it("should produce a unique set from a column") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           || TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           || BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           || NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           || ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           || NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || WRKR   | AMEX     |   100.12 | 2022-09-04T09:36:48.459Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           ||---------------------------------------------------------|
           |select symbol: unique(symbol)
           |from @stocks
           |order by symbol
           |""".stripMargin)
      assert(results.toMapGraph.map(_.map { case (k: String, v: Array[_]) => k -> v.toSet }) == List(
        Map("symbol" -> Set("BOOTY", "ABC", "AAPL", "AQKU", "NGA", "WRKR", "BKBK", "NFRK", "NGINX", "ESCN", "TREE", "TRX"))
      ))
    }

    it("should perform aggregate queries") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        s"""|select tickers: count(unique(symbol))
            |from (
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
            |""".stripMargin)
      assert(results.toMapGraph == List(Map("tickers" -> 12)))
    }

    it("should produce the set of distinct rows via group by") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           || TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           || BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           || NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           || ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           || NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || WRKR   | AMEX     |   100.12 | 2022-09-04T09:36:48.459Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           ||---------------------------------------------------------|
           |select symbol, exchange, lastSale, lastSaleTime
           |from @stocks
           |group by symbol, exchange, lastSale, lastSaleTime
           |order by symbol
           |""".stripMargin)
      assert(results.tabulate().mkString("\n") ==
        """||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           || BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           || ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           || NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           || NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           || NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           || WRKR   | AMEX     |   100.12 | 2022-09-04T09:36:48.459Z |
           ||---------------------------------------------------------|""".stripMargin)
    }

    it("should produce the set of distinct rows") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|stocks =
           ||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           || TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           || BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           || NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           || ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           || NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || WRKR   | AMEX     |   100.12 | 2022-09-04T09:36:48.459Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           ||---------------------------------------------------------|
           |stocks.distinct()
           |""".stripMargin)
      assert(results.tabulate().mkString("\n") ==
        """||---------------------------------------------------------|
           || symbol | exchange | lastSale | lastSaleTime             |
           ||---------------------------------------------------------|
           || ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           || BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           || TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           || BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           || NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           || TRX    | NYSE     |    88.22 | 2022-09-04T09:12:53.706Z |
           || NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           || WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           || ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           || NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           || AAPL   | NASDAQ   |   100.01 | 2022-09-04T09:36:46.033Z |
           || WRKR   | AMEX     |   100.12 | 2022-09-04T09:36:48.459Z |
           ||---------------------------------------------------------|""".stripMargin)
    }

  }

}
