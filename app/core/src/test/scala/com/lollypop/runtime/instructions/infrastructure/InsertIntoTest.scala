package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.{RowsOfValues, Select, TableLiteral}
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class InsertIntoTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[InsertInto].getSimpleName) {

    it("should compile insert instructions referencing inner-tables") {
      val results = compiler.compile(
        """|insert into stocks#transactions (price, transactionTime)
           |values (35.11, "2021-08-05T19:23:12.000Z"),
           |       (35.83, "2021-08-05T19:23:15.000Z"),
           |       (36.03, "2021-08-05T19:23:17.000Z")
           |where symbol is 'AMD'
           |""".stripMargin)
      assert(results == InsertInto(
        ref = DatabaseObjectRef("stocks#transactions"),
        source = RowsOfValues(values = List(
          List(35.11.v, "2021-08-05T19:23:12.000Z".v),
          List(35.83.v, "2021-08-05T19:23:15.000Z".v),
          List(36.03.v, "2021-08-05T19:23:17.000Z".v)
        )),
        fields = Seq("price".f, "transactionTime".f),
        condition = "symbol".f is "AMD",
        limit = None
      ))
    }

    it("should decompile insert instructions referencing inner-tables") {
      val model = InsertInto(
        ref = DatabaseObjectRef("stocks#transactions"),
        source = RowsOfValues(values = List(
          List(35.11.v, "2021-08-05T19:23:12.000Z".v),
          List(35.83.v, "2021-08-05T19:23:15.000Z".v),
          List(36.03.v, "2021-08-05T19:23:17.000Z".v)
        )),
        fields = Seq("price".f, "transactionTime".f),
        condition = "symbol".f is "AMD",
        limit = None
      )
      assert(model.toSQL ==
        """|insert into stocks#transactions (price, transactionTime)
           |values (35.11,"2021-08-05T19:23:12.000Z"), (35.83,"2021-08-05T19:23:15.000Z"), (36.03,"2021-08-05T19:23:17.000Z")
           |where symbol is "AMD"
           |""".stripMargin.replaceAll("\n", " ").trim)
    }

    it("should execute insert instructions referencing inner-tables") {
      val table = DatabaseObjectRef("injectTest")
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $table
            |create table $table (symbol: String(8), exchange: String(8), transactions: Table(price: Double, transactionTime: DateTime))
            |insert into $table (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
            |
            |insert into $table#transactions (price, transactionTime)
            |values (35.11, "2021-08-05T19:23:12.000Z"),
            |       (35.83, "2021-08-05T19:23:15.000Z"),
            |       (36.03, "2021-08-05T19:23:17.000Z")
            |where symbol is 'AMD'
            |""".stripMargin)
      assert(cost.inserted == 8)
      assert(scope.getRowCollection(table).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 156.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should support insert without explicitly defined fields") {
      val results = compiler.compile(
        "insert into Students values ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
      assert(results == InsertInto(DatabaseObjectRef("Students"), RowsOfValues(values = List(
        List("Fred Flintstone", 35.0, 1.28),
        List("Barney Rubble", 32.0, 2.32)
      ))))
    }

    it("should support insert-into-select") {
      val results = compiler.compile(
        """|insert into OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |from Securities
           |where Industry == 'Oil/Gas Transmission'
           |""".stripMargin)
      val fields = List("Symbol".f, "Name".f, "LastSale".f, "MarketCap".f, "IPOyear".f, "Sector".f, "Industry".f)
      assert(results == InsertInto(DatabaseObjectRef("OilGasSecurities"),
        Select(
          fields = fields,
          from = DatabaseObjectRef("Securities"),
          where = "Industry".f === "Oil/Gas Transmission"),
        fields = fields
      ))
    }

    it("should support insert-into from a clause") {
      val results = compiler.compile(
        """|insert into OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |values
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
      val fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "LastSale".f)
      assert(results == InsertInto(DatabaseObjectRef("OilGasSecurities"),
        RowsOfValues(
          values = List(
            List("AAPL", "Apple, Inc.", "Technology", "Computers", 203.45),
            List("AMD", "American Micro Devices, Inc.", "Technology", "Computers", 22.33)
          )),
        fields = fields
      ))
    }

    it("should support insert-into from Table literal") {
      val results = compiler.compile(
        """|insert into OilGasSecurities
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | AAXX   | NYSE     |    56.12 |
           |  | UPEX   | NYSE     |   116.24 |
           |  | XYZ    | AMEX     |    31.95 |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |""".stripMargin)
      assert(results == InsertInto(DatabaseObjectRef("OilGasSecurities"),
        TableLiteral(List(
          List("symbol", "exchange", "lastSale"),
          List("AAXX", "NYSE", "56.12"),
          List("UPEX", "NYSE", "116.24"),
          List("XYZ", "AMEX", "31.95"),
          List("JUNK", "AMEX", "97.61")
        ))
      ))
    }

    it("should support decompiling insert without explicitly defined fields") {
      verify("insert into Students values ('Fred Flintstone', 35, 1.28), ('Barney Rubble', 32, 2.32)")
    }

    it("should support decompiling insert-into-select") {
      verify(
        """|insert into OilGasSecurities (Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry)
           |select Symbol, Name, LastSale, MarketCap, IPOyear, Sector, Industry
           |from Securities
           |where Industry == 'Oil/Gas Transmission'
           |""".stripMargin)
    }

    it("should support decompiling insert-into-values") {
      verify(
        """|insert into OilGasSecurities (Symbol, Name, Sector, Industry, LastSale)
           |values
           |  ('AAPL', 'Apple, Inc.', 'Technology', 'Computers', 203.45),
           |  ('AMD', 'American Micro Devices, Inc.', 'Technology', 'Computers', 22.33)
           |""".stripMargin)
    }

    /////////////////////////////////////////////////////////////////////////////////////////
    //    NESTED table INSERTS
    /////////////////////////////////////////////////////////////////////////////////////////

    it(s"should insert JSON data into a nested table on disk") {
      val stocks = DatabaseObjectRef("temp.jdbc.StocksDemoTestA")
      val (scope0, res0_?, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $stocks
            |create table $stocks (
            |   symbol: String(4),
            |   exchange: String(6),
            |   transactions Table (price Double, transactionTime DateTime)[2]
            |)
            |""".stripMargin)
      assert(res0_?.inserted == 0)

      val (scope1, cost, _) = LollypopVM.executeSQL(scope0,
        s"""|insert into $stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', '{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', '{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('INTC','NYSE', '{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', '{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', '[{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]')
            |""".stripMargin)
      assert(cost.inserted == 5)

      // verify the count
      val (_, _, res2) = LollypopVM.searchSQL(scope1, s"select total: count(*) from $stocks")
      assert(res2.toMapGraph == List(Map("total" -> 5)))
    }

    it(s"should insert objects into a nested table on disk") {
      val stocks = DatabaseObjectRef("temp.jdbc.StocksDemoTestB")
      val (scope0, res0_?, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $stocks
            |create table $stocks (
            |   symbol: String(4),
            |   exchange: String(6),
            |   transactions Table (price Double, transactionTime DateTime)[2]
            |)
            |""".stripMargin)
      assert(res0_?.inserted == 0)

      val (scope1, res1_?, _) = LollypopVM.executeSQL(scope0,
        s"""|insert into $stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', [{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('AMD', 'NASDAQ', [{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('INTC','NYSE', [{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('AMZN', 'NASDAQ', [{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
            |""".stripMargin)
      assert(res1_?.inserted == 5)

      // verify the count
      val (_, _, res2) = LollypopVM.searchSQL(scope1, s"select total: count(*) from $stocks")
      assert(res2.toMapGraph == List(Map("total" -> 5)))
    }

    it(s"should insert JSON data into a nested table in memory") {
      val (scope0, result0, _) = LollypopVM.executeSQL(Scope(), sql =
        s"""|declare table stocks(symbol: String(4), exchange: String(6), transactions: Table(price Double, transactionTime DateTime)[2])
            |insert into @stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', '{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', '{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('INTC','NYSE', '{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', '{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', '[{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]')
            |""".stripMargin)
      assert(result0.inserted == 5)

      // verify the count
      val (_, _, result1) = LollypopVM.searchSQL(scope0, sql = "select total: count(*) from @stocks")
      assert(result1.toMapGraph == List(Map("total" -> 5)))
    }

    it("should insert objects into a nested table in memory") {
      val (scope0, result0, _) = LollypopVM.executeSQL(Scope(), sql =
        s"""|declare table stocks(symbol: String(4), exchange: String(6), transactions: Table(price: Double, transactionTime: DateTime)[2])
            |insert into @stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', [{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('AMD', 'NASDAQ', [{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('INTC','NYSE', [{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('AMZN', 'NASDAQ', [{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}]),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
            |""".stripMargin)
      assert(result0.inserted == 5)

      // verify the count
      val (_, _, result1) = LollypopVM.searchSQL(scope0, s"select total: count(*) from @stocks")
      assert(result1.toMapGraph == List(Map("total" -> 5)))
    }

    it("should insert objects into a table variable") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|declare table results(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @results (symbol, exchange, lastSale)
           |values ('GMTQ', 'OTCBB', 0.1111), ('ABC', 'NYSE', 38.47), ('GE', 'NASDAQ', 57.89)
           |@results
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("exchange" -> "OTCBB", "symbol" -> "GMTQ", "lastSale" -> 0.1111),
        Map("exchange" -> "NYSE", "symbol" -> "ABC", "lastSale" -> 38.47),
        Map("exchange" -> "NASDAQ", "symbol" -> "GE", "lastSale" -> 57.89)
      ))
    }

  }

}
