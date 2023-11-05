package com.lollypop.runtime.instructions.queryables

import com.lollypop.language._
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.language.models._
import com.lollypop.runtime.datatypes.{DateTimeType, Float32Type}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.Dictionary
import com.lollypop.runtime.instructions.expressions.aggregation.{Avg, Count, Unique}
import com.lollypop.runtime.instructions.invocables._
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class SelectTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Select].getSimpleName) {

    it("should parse direct query tags (%Q) - Direct Table") {
      verify(text = "select firstName, lastName from AddressBook", template = "%Q:query")(SQLTemplateParams(instructions = Map(
        "query" -> Select(fields = List("firstName".f, "lastName".f), from = DatabaseObjectRef("AddressBook"))
      )))
    }

    it("should parse query source (queries, tables and variables) tags (%q)") {
      verify(text = "( select firstName, lastName from AddressBook )", template = "%q:query")(SQLTemplateParams(instructions = Map(
        "query" -> Select(fields = List("firstName".f, "lastName".f), from = DatabaseObjectRef("AddressBook"))
      )))
      verify(text = "AddressBook", template = "%q:table")(SQLTemplateParams(instructions = Map(
        "table" -> DatabaseObjectRef("AddressBook")
      )))
      verify(text = "@addressBook", template = "%q:variable")(SQLTemplateParams(instructions = Map(
        "variable" -> @@("addressBook")
      )))
    }

    it("should parse insert values (queries, values and variables) tags (%V)") {
      verify(text = "( select * from AddressBook )", template = "%V:query")(SQLTemplateParams(instructions = Map(
        "query" -> Select(fields = List("*".f), from = DatabaseObjectRef("AddressBook"))
      )))
      verify(text = "values (1, 2, 3)", template = "%V:values")(SQLTemplateParams(instructions = Map(
        "values" -> RowsOfValues(List(List(1d, 2d, 3d)))
      )))
      verify(text = "@addressBook", template = "%V:variable")(SQLTemplateParams(instructions = Map(
        "variable" -> @@("addressBook")
      )))
    }

    it("should support select without a from clause") {
      val results = compiler.compile(
        """|select symbol: 'GMTQ',
           |       exchange: 'OTCBB',
           |       lastSale: Float('0.1111'),
           |       lastSaleTime: DateTime(1631508164812)
           |""".stripMargin)
      assert(results == Select(fields = Seq(
        "GMTQ" as "symbol",
        "OTCBB" as "exchange",
        Float32Type("0.1111") as "lastSale",
        DateTimeType(1631508164812L) as "lastSaleTime"
      )))
    }

    it("should support select dictionary without a from clause") {
      val results = compiler.compile(
        """|select {
           |  symbol: 'GMTQ',
           |  exchange: 'OTCBB',
           |  lastSale: 0.1111,
           |  lastSaleTime: DateTime(1631508164812)
           |} as dict
           |""".stripMargin)
      assert(results == Select(fields = Seq(
        Dictionary(
          "symbol" -> "GMTQ".v,
          "exchange" -> "OTCBB".v,
          "lastSale" -> 0.1111.v,
          "lastSaleTime" -> DateTimeType(1631508164812L.v)
        ) as "dict"
      )))
    }

    it("should support select from a variable source") {
      val results = compiler.compile(
        """|select * from @results where symbol == 'ABC'
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("*".f),
        from = Some(@@("results")),
        where = "symbol".f === "ABC"
      ))
    }

    it("should support select function call") {
      val results = compiler.compile(
        """|select symbol, customFx(lastSale) as rating
           |from Securities
           |where naics == '12345'
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("symbol".f, "customFx".fx("lastSale".f) as "rating"),
        from = DatabaseObjectRef("Securities"),
        where = "naics".f === "12345"
      ))
    }

    it("should support select .. group by fields") {
      val results = compiler.compile(
        """|select Sector, Industry, LastSale: avg(LastSale), total: count(*), distinctTotal: count(unique(*))
           |from Customers
           |group by Sector, Industry
           |""".stripMargin)
      assert(results == Select(
        fields = List("Sector".f, "Industry".f, Avg("LastSale".f) as "LastSale", Count("*".f) as "total",
          Count(Unique("*".f)) as "distinctTotal"),
        from = DatabaseObjectRef("Customers"),
        groupBy = List("Sector".f, "Industry".f)
      ))
    }

    it("should support select .. group by indices") {
      val results = compiler.compile(
        """|select Sector, Industry, avg(LastSale) as LastSale, count(*) as total, count(unique(*)) as distinctTotal
           |from Customers
           |group by 1, 2
           |""".stripMargin)
      assert(results == Select(
        fields = List("Sector".f, "Industry".f, Avg("LastSale".f) as "LastSale", Count("*".f) as "total",
          Count(Unique("*".f)) as "distinctTotal"),
        from = DatabaseObjectRef("Customers"),
        groupBy = List("1".f, "2".f)
      ))
    }

    it("should support select .. limit n") {
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |limit 100
           |""".stripMargin)
      assert(results == Select(
        fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f),
        from = DatabaseObjectRef("Customers"),
        where = "Industry".f === "Oil/Gas Transmission",
        limit = Some(100)
      ))
    }

    it("should support select .. order by") {
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |order by Symbol desc, Name asc
           |""".stripMargin)
      assert(results == Select(
        fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
        from = DatabaseObjectRef("Customers"),
        where = "Industry".f === "Oil/Gas Transmission",
        orderBy = List("Symbol".desc, "Name".asc)
      ))
    }

    it("should support set variable") {
      val results = compiler.compile("set $customers = $customers + 1")
      assert(results == SetAnyVariable(ref = "customers".f, $("customers") + 1))
    }

    it("should support set row variable") {
      val results = compiler.compile(
        """|set securities = (
           |  select Symbol, Name, Sector, Industry, `Summary Quote`
           |  from Securities
           |  where Industry == 'Oil/Gas Transmission'
           |  order by Symbol asc
           |)
           |""".stripMargin)
      assert(results == SetAnyVariable(ref = "securities".f,
        Select(
          fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "Summary Quote".f),
          from = DatabaseObjectRef("Securities"),
          where = "Industry".f === "Oil/Gas Transmission",
          orderBy = List("Symbol".asc)
        )))
    }

    it("should support help") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|select example from (help('new'))
           |""".stripMargin)
      results.tabulate().foreach(logger.info)
      assert(results.tabulate().exists(_ contains "new `java.util.Date`()"))
    }

    it("should decompile select from a variable source") {
      verify(
        """|select * from @results where symbol == 'ABC'
           |""".stripMargin)
    }

    it("should decompile select function call") {
      verify(
        """|select symbol, customFx(lastSale) as rating
           |from Securities
           |where naics is '12345'
           |""".stripMargin)
    }

    it("should decompile select .. group by fields") {
      verify(
        """|select Sector, Industry, avg(LastSale) as LastSale, count(*) as total, count(unique(*)) as distinctTotal
           |from Customers
           |group by Sector, Industry
           |""".stripMargin)
    }

    it("should decompile select .. group by indices") {
      verify(
        """|select Sector, Industry, avg(LastSale) as LastSale, count(*) as total, count(unique(*)) as distinctTotal
           |from Customers
           |group by 1, 2
           |""".stripMargin)
    }

    it("should decompile select .. limit n") {
      verify(
        """|select Symbol, Name, Sector, Industry
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |limit 100
           |""".stripMargin)
    }

    it("should decompile select .. order by") {
      verify(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where Industry == 'Oil/Gas Transmission'
           |order by Symbol desc, Name asc
           |""".stripMargin)
    }

    it("should execute queries against table literals") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|select * from (
           |    |-------------------------------------------------------------------------|
           |    | ticker | market | lastSale | roundedLastSale | lastSaleTime             |
           |    |-------------------------------------------------------------------------|
           |    | NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
           |    | AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
           |    | WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
           |    | ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
           |    | NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
           |    |-------------------------------------------------------------------------|
           |) where market is 'OTCBB'
           |""".stripMargin)
      assert(device.toMapGraph == List(Map(
        "market" -> "OTCBB", "roundedLastSale" -> 98.9, "lastSale" -> 98.9501,
        "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.846Z"), "ticker" -> "NKWI"
      )))
    }

    it("should perform aggregate queries") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        s"""|namespace 'samples.stocks'
            |select
            |   market: exchange,
            |   total: count(*),
            |   tickers: count(unique(symbol)),
            |   avgLastSale: avg(lastSale),
            |   minLastSale: min(lastSale),
            |   maxLastSale: max(lastSale),
            |   sumLastSale: sum(lastSale)
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
            |group by exchange
            |order by market desc
            |""".stripMargin)
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(
        Map("market" -> "OTCBB", "sumLastSale" -> 46.90976, "maxLastSale" -> 17.5776, "total" -> 6, "tickers" -> 4, "avgLastSale" -> 7.818293333333333, "minLastSale" -> 0.00123),
        Map("market" -> "NYSE", "sumLastSale" -> 205.0608, "maxLastSale" -> 88.56, "total" -> 3, "tickers" -> 2, "avgLastSale" -> 68.3536, "minLastSale" -> 28.2808),
        Map("market" -> "NASDAQ", "sumLastSale" -> 370.2295, "maxLastSale" -> 100.12, "total" -> 5, "tickers" -> 4, "avgLastSale" -> 74.04589999999999, "minLastSale" -> 23.6812),
        Map("market" -> "AMEX", "sumLastSale" -> 189.5489, "maxLastSale" -> 100.12, "total" -> 3, "tickers" -> 2, "avgLastSale" -> 63.182966666666665, "minLastSale" -> 42.5934)
      ))
    }

    it("should select rows from an array of dictionaries") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|select * from ([{
           |  symbol: 'ABC',
           |  exchange: 'NYSE',
           |  lastSale: 56.98,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC"]
           |},{
           |  symbol: 'GE',
           |  exchange: 'NASDAQ',
           |  lastSale: 83.13,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["XYZ"]
           |},{
           |  symbol: 'GMTQ',
           |  exchange: 'OTCBB',
           |  lastSale: 0.1111,
           |  lastSaleTime: DateTime(1631508164812),
           |  codes: ["ABC", "XYZ"]
           |}])
           |""".stripMargin
      )
      assert(results.tabulate() ==
        """||--------------------------------------------------------------------------|
           || exchange | symbol | codes          | lastSale | lastSaleTime             |
           ||--------------------------------------------------------------------------|
           || NYSE     | ABC    | ["ABC"]        |    56.98 | 2021-09-13T04:42:44.812Z |
           || NASDAQ   | GE     | ["XYZ"]        |    83.13 | 2021-09-13T04:42:44.812Z |
           || OTCBB    | GMTQ   | ["ABC", "XYZ"] |   0.1111 | 2021-09-13T04:42:44.812Z |
           ||--------------------------------------------------------------------------|
           |""".stripMargin.trim.split("\n").toList)
    }

  }

  def verify(text: String, template: String)(expected: SQLTemplateParams): Assertion = {
    info(s"'$template' <~ '$text'")
    val actual = SQLTemplateParams(TokenStream(text), template)
    println(s"actual:   ${actual.parameters}")
    println(s"expected: ${expected.parameters}")
    assert(actual == expected, s"'$text' ~> '$template' failed")
  }

}
