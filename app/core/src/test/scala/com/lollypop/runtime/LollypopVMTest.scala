package com.lollypop.runtime

import com.lollypop.language.dieNoResultSet
import com.lollypop.runtime.datatypes.Int32Type
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.lollypop.runtime.errors.DurableObjectNotFound
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.OptionHelper.implicits.risky.value2Option
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import lollypop.io.IOCost

import java.util.Date

/**
 * Database VM Test Suite
 */
class LollypopVMTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[LollypopVM.type].getSimpleName) {
    val insertCount = 5000

    ////////////////////////////////////////////////////////////////////////////
    //    Basic expression tests
    ////////////////////////////////////////////////////////////////////////////

    it(s"should support string interpolation") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(), sql =
        """|var name: String(20) = 'Larry Jerry'
           |var age: Byte = 32 + 3
           |var title: String = 'name: {{ name }}, age: {{ age }}'
           |""".stripMargin)
      assert(scope.resolveAs("title") contains "name: Larry Jerry, age: 35")
    }

    it("should support infix accessor: ({ total: 100 }).total") {
      val (_, _, value) = LollypopVM.executeSQL(Scope(), sql = "({ total: 100 }).total")
      assert(value == 100)
    }

    it("""should support virtual infix accessor: ({ symbol: "ABC", exchange: "NYSE" }).symbol""") {
      val (_, _, value) = LollypopVM.executeSQL(Scope(), sql = """({ symbol: "ABC", exchange: "NYSE" }).symbol""")
      assert(value == "ABC")
    }

    it("should support dictionary infix accessor: { estimate: ({ total: 1619 }}.estimate).total") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql = "select n: ({ estimate: { total: 1619 }}.estimate).total")
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("n" -> 1619)))
    }

    ////////////////////////////////////////////////////////////////////////////
    //    Basic query tests
    ////////////////////////////////////////////////////////////////////////////

    it("should perform select Interval (idiomatic)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|DateTime(1632702853166) + Interval('-7 DAYS')
           |""".stripMargin
      )
      assert(result.collect { case d: Date => d.getTime } contains 1632098053166L)
    }

    it("should be able to produce records containing Random.nextInt(om) data") {
      val (_, _, device_?) = LollypopVM.executeSQL(Scope(),
        """|declare table stockQuotes(symbol: String(4), exchange: String(5), lastSale: Double, lastSaleTime: DateTime)
           |insert into @stockQuotes (lastSaleTime, lastSale, exchange, symbol)
           |select lastSaleTime: new `java.util.Date`(),
           |       lastSale: Random.nextDouble(0.99),
           |       exchange: 'OTCBB',
           |       symbol: Random.nextString(['A' to 'Z'], 4)
           |stockQuotes
           |""".stripMargin)
      device_?.collect { case d: RowCollection => d }.foreach(_.tabulate() foreach logger.info)
    }

    it(s"should fail if an object that does not exist is queried") {
      assertThrows[DurableObjectNotFound] {
        LollypopVM.searchSQL(Scope(),
          s"""|drop if exists NotReallyThere
              |describe ns("NotReallyThere")
              |""".stripMargin)
      }
    }

    ////////////////////////////////////////////////////////////////////////////
    //    VOLUME tests
    ////////////////////////////////////////////////////////////////////////////

    it(s"should describe the structure of Table stocks_B") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql =
        s"""|namespace 'temp.stocks'
            |drop if exists stocks_B
            |create table stocks_B (
            |   symbol: String(6),
            |   exchange: String(6),
            |   lastSale: Double,
            |   lastSaleTime: DateTime
            |)
            |describe ns("stocks_B")
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("type" -> "String(6)", "name" -> "symbol"),
        Map("type" -> "String(6)", "name" -> "exchange"),
        Map("type" -> "Double", "name" -> "lastSale"),
        Map("type" -> "DateTime", "name" -> "lastSaleTime")
      ))
    }

    it(s"should insert rows into the Table stocks_B") {
      val scope0 = Scope()
        .withVariable(name = "cnt", `type` = Int32Type, value = Some(0), isReadOnly = false)
        .withVariable(name = "insertCount", value = insertCount)
      val (scope1, _, _) = LollypopVM.executeSQL(scope0,
        s"""|namespace 'temp.stocks'
            |val stocks_B = ns('temp.stocks.stocks_B')
            |var counter: Int = 0
            |stocks_B.setLength(0)
            |[1 to insertCount].foreach((cnt: Int) => {
            |   insert into stocks_B (lastSaleTime, lastSale, exchange, symbol)
            |   values (
            |     DateTime(),
            |     scaleTo(100 * Random.nextDouble(0.99), 4),
            |     ["AMEX", "NASDAQ", "NYSE", "OTCBB"][Random.nextInt(4)],
            |     Random.nextString(['A' to 'Z'], 4)
            |   )
            |   counter += 1
            |})
            |counter
            |""".stripMargin)
      assert(scope1.resolve("counter") contains insertCount)
    }

    it(s"should perform transformation queries") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |select
            |   ticker: symbol,
            |   market: exchange,
            |   lastSale,
            |   roundedLastSale: scaleTo(lastSale, 1),
            |   lastSaleTime
            |from stocks_B
            |order by lastSale desc
            |limit 5
            |""".stripMargin)
      val device = result.collect { case d: RowCollection => d } || dieNoResultSet()
      assert(device.columns.map(_.name).toSet == Set("ticker", "market", "lastSale", "roundedLastSale", "lastSaleTime"))
      assert(device.getLength == 5)
      device.tabulate() foreach logger.info
    }

    it(s"should perform aggregate queries using HAVING") {
      val (_, _, device_?) = LollypopVM.executeSQL(Scope(),
        s"""|declare table Travelers(id UUID, lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @Travelers (id, lastName, firstName, destAirportCode)
            |values (UUID(), 'JONES', 'GARRY', 'SNA'), (UUID(), 'JONES', 'DEBBIE', 'SNA'),
            |       (UUID(), 'JONES', 'TAMERA', 'SNA'), (UUID(), 'JONES', 'ERIC', 'SNA'),
            |       (UUID(), 'ADAMS', 'KAREN', 'DTW'), (UUID(), 'ADAMS', 'MIKE', 'DTW'),
            |       (UUID(), 'JONES', 'SAMANTHA', 'BUR'), (UUID(), 'SHARMA', 'PANKAJ', 'LAX')
            |
            |select lastName, members: count(*)
            |from @Travelers
            |group by lastName
            |having members > 2
            |order by lastName
            |""".stripMargin)
      assert(device_?.nonEmpty)
      device_?.collect { case d: RowCollection => d }.foreach(_.tabulate() foreach logger.info)
      assert(device_?.collect { case d: RowCollection => d }.get.toMapGraph == List(Map("lastName" -> "JONES", "members" -> 5)))
    }

    ////////////////////////////////////////////////////////////////////////////
    //    Nested Table tests
    ////////////////////////////////////////////////////////////////////////////

    it("should insert test data into a new Table") {
      val (_, stat, _) = LollypopVM.executeSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |drop if exists stocks_C
            |create table stocks_C (
            |   symbol: String(4),
            |   exchange: String(8),
            |   lastSale: Double,
            |   transactions: Table (
            |      price: Double,
            |      transactionTime: DateTime
            |   )[2]
            |)
            |insert into stocks_C (symbol, exchange, lastSale, transactions)
            |values ('AAPL', 'NASDAQ', 156.39, '{"price":156.39, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', 56.87, '{"price":56.87, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('INTC','NYSE', 89.44, '{"price":89.44, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('GMTQ', 'OTCBB', 0.1111, '{"price":0.1111, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', 988.12, '{"price":988.12, "transactionTime":"2021-08-06T15:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', 0.0011, '[{"price":0.0010, "transactionTime":"2021-08-06T15:23:11.000Z"},
            |                                   {"price":0.0011, "transactionTime":"2021-08-06T15:23:12.000Z"}]')
            |""".stripMargin)
      assert(stat.inserted == 6)
    }

    it("should count a column (slower)") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql =
        s"""|namespace 'temp.stocks'
            |select total: count(symbol) from stocks_C
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("total" -> 6)))
    }

    it("should count all rows (faster)") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql =
        s"""|namespace 'temp.stocks'
            |select total: count(*) from stocks_C
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("total" -> 6)))
    }

    it("should select specific fields with a source") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), sql =
        s"""|namespace 'temp.stocks'
            |select symbol, lastSale from stocks_C
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAPL", "lastSale" -> 156.39),
        Map("symbol" -> "AMD", "lastSale" -> 56.87),
        Map("symbol" -> "INTC", "lastSale" -> 89.44),
        Map("symbol" -> "GMTQ", "lastSale" -> 0.1111),
        Map("symbol" -> "AMZN", "lastSale" -> 988.12),
        Map("symbol" -> "SHMN", "lastSale" -> 0.0011)
      ))
    }

    it("should select specific fields and filter rows via a where clause") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |select symbol, exchange
            |from stocks_C
            |where exchange.matches("NY.E")
            |order by symbol asc
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("symbol" -> "INTC", "exchange" -> "NYSE")
      ))
    }

    it("should perform select with group by") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |select exchange, total: count(*)
            |from stocks_C
            |group by exchange
            |order by total asc
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("exchange" -> "NYSE", "total" -> 1),
        Map("exchange" -> "OTCBB", "total" -> 2),
        Map("exchange" -> "NASDAQ", "total" -> 3)
      ))
    }

    it("should perform select avg, min, max and sum with group by") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |select count(*) as total,
            |       avg(lastSale) as avgLastSale,
            |       max(lastSale) as maxLastSale,
            |       min(lastSale) as minLastSale,
            |       sum(lastSale) as sumLastSale
            |from stocks_C
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("total" -> 6, "avgLastSale" -> 215.15536666666665, "maxLastSale" -> 988.12, "minLastSale" -> 0.0011, "sumLastSale" -> 1290.9322)
      ))
    }

    it("should perform select with group by and aggregate functions") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|namespace 'temp.stocks'
            |select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
            |from stocks_C
            |group by exchange
            |order by total asc
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("total" -> 1, "exchange" -> "NYSE", "maxPrice" -> 89.44, "minPrice" -> 89.44),
        Map("total" -> 2, "exchange" -> "OTCBB", "maxPrice" -> 0.1111, "minPrice" -> 0.0011),
        Map("total" -> 3, "exchange" -> "NASDAQ", "maxPrice" -> 988.12, "minPrice" -> 56.87)
      ))
    }

    it("should use .compact() to remove deleted rows from a table") {
      val (_, cost, _) = LollypopVM.executeSQL(Scope(),
        """|set stocks = {
           |    set total = 1000
           |    declare table myQuotes(symbol: String(5), exchange: String(6), lastSale: Float, lastSaleTime: DateTime)[1000]
           |    [1 to total].foreach((cnt: Int) => {
           |        insert into @myQuotes (lastSaleTime, lastSale, exchange, symbol)
           |        select lastSaleTime: new `java.util.Date`(),
           |               lastSale: scaleTo(150 * Random.nextDouble(0.99), 4),
           |               exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
           |               symbol: Random.nextString(['A' to 'Z'], 5)
           |    })
           |
           |    delete from @myQuotes
           |    where __id between 0 and 25
           |    or    __id between 50 and 100
           |    or    __id between 250 and 300
           |    or    __id between 500 and 700
           |    or    __id between 950 and 1000
           |    myQuotes
           |}
           |
           |stocks.compact()
           |""".stripMargin)
      cost.toRowCollection.tabulate().foreach(logger.info)
      assert(cost == IOCost.empty)
    }

    it("should use .swap() to swap the contents of two rows in a table") {
      val (_, _, returned) = LollypopVM.executeSQL(Scope(),
        s"""|declare table Travelers(id: UUID, lastName: String(12), firstName: String(12), destAirportCode: String(3))
            | containing values
            |       (UUID(), 'JONES', 'GARRY', 'SNA'),
            |       (UUID(), 'JONES', 'DEBBIE', 'SNA'),
            |       (UUID(), 'JONES', 'TAMERA', 'SNA'),
            |       (UUID(), 'JONES', 'ERIC', 'SNA'),
            |       (UUID(), 'ADAMS', 'KAREN', 'DTW'),
            |       (UUID(), 'ADAMS', 'MIKE', 'DTW'),
            |       (UUID(), 'JONES', 'SAMANTHA', 'BUR'),
            |       (UUID(), 'SHARMA', 'PANKAJ', 'LAX')
            |
            |Travelers.swap(1, 2)
            |select firstName, lastName from @Travelers limit 3
            |""".stripMargin)
      val device_? = Option(returned).collect { case device: RowCollection => device }.toList
      device_?.flatMap(_.tabulate()) foreach logger.info
      assert(device_?.flatMap(_.toMapGraph) == List(
        Map("firstName" -> "GARRY", "lastName" -> "JONES"),
        Map("firstName" -> "TAMERA", "lastName" -> "JONES"),
        Map("firstName" -> "DEBBIE", "lastName" -> "JONES")
      ))
    }


  }

}
