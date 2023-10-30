package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.Literal
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.{AND, WhereIn}
import com.lollypop.runtime.instructions.invocables.{ScopeModificationBlock, SetAnyVariable}
import com.lollypop.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.lollypop.runtime.instructions.operators.Plus
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import lollypop.io.{IOCost, RowIDRange}

class UpdateTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[Update].getSimpleName) {

    it("should compile update instructions") {
      val results = LollypopCompiler().compile(
        """|update stocks#transactions
           |set price = 0.0012
           |where symbol is 'SHMN'
           |and transactions wherein (price is 0.001)
           |limit 1
           |""".stripMargin)
      assert(results == Update(
        ref = DatabaseObjectRef("stocks#transactions"),
        modification = SetAnyVariable("price".f, 0.0012.v),
        condition = AND("symbol".f is "SHMN", WhereIn("transactions".f, "price".f is 0.001.v)),
        limit = Some(1)
      ))
    }

    it("should render itself as SQL") {
      val model = Update(
        ref = DatabaseObjectRef("stocks#transactions"),
        modification = SetAnyVariable("price".f, 0.0012.v),
        condition = AND("symbol".f is "SHMN".v, WhereIn("transactions".f, "price".f is 0.001.v)),
        limit = Some(1.v))
      assert(model.toSQL ==
        """|update stocks#transactions
           |set price = 0.0012
           |where (symbol is "SHMN")
           |and (transactions wherein price is 0.001)
           |limit 1
           |""".stripMargin.replaceAll("\n", " ").trim)
    }

    it("should update rows from a clustered inner-table") {
      val ref = DatabaseObjectRef("stocks")
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (
            |       price Double,
            |       transactionTime DateTime
            |   )[5]
            |)
            |
            |insert into $ref (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
            |
            |update $ref#transactions
            |set price = 0.0012
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |limit 1
            |""".stripMargin)
      assert(cost.updated == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 156.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0012, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should support compilation with +=") {
      val expected = Update(
        ref = DatabaseObjectRef("Stocks"),
        modification = Plus(a = "lastSale".f, b = 0.0001.v).doAndSet,
        condition = "exchange".f is "OTCBB",
        limit = Some(Literal(25)))
      val actual = LollypopCompiler().compile(
        """|update Stocks
           |lastSale += 0.0001
           |where exchange is 'OTCBB'
           |limit 25
           |""".stripMargin)
      assert(expected == actual)
    }

    it("should support compilation with set") {
      val expected = Update(
        ref = DatabaseObjectRef("Stocks"),
        modification = ScopeModificationBlock(
          SetAnyVariable(ref = "name".f, instruction = "Apple, Inc.".v),
          SetAnyVariable(ref = "sector".f, instruction = "Technology".v),
          SetAnyVariable(ref = "industry".f, instruction = "Computers".v),
          SetAnyVariable(ref = "lastSale".f, instruction = 203.45.v)),
        condition = "symbol".f is "AAPL",
        limit = Some(1.v))
      val actual = LollypopCompiler().compile(
        """|update Stocks
           |set name = 'Apple, Inc.', sector = 'Technology', industry = 'Computers', lastSale = 203.45
           |where symbol is 'AAPL'
           |limit 1
           |""".stripMargin)
      assert(expected == actual)
    }

    it("should support decompilation") {
      val model = LollypopCompiler().nextOpCodeOrDie(TokenStream(
        """|update Stocks
           |set lastSale = 1.06
           |where symbol is 'ABC'
           |limit 1
           |""".stripMargin))
      assert(model.toSQL == "update Stocks set lastSale = 1.06 where symbol is \"ABC\" limit 1")
    }

    it("should support execution using set") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        """|declare table travelers(
           |  id RowNumber,
           |  position Int,
           |  lastName String(32),
           |  firstName String(32),
           |  airportCode String(3))[5]
           |
           |insert into @@travelers (position, lastName, firstName, airportCode)
           |values (5, 'JONES', 'GARRY', 'SNA'),
           |       (6, 'JONES', 'DEBBIE', 'SNA'),
           |       (8, 'JONES', 'TAMERA', 'SNA'),
           |       (7, 'JONES', 'ERIC', 'SNA') &&
           |
           |update @@travelers
           |set position = 4
           |where lastName is 'JONES' and firstName is 'TAMERA'
           |limit 1
           |""".stripMargin)
      assert(cost == IOCost(created = 1, inserted = 4, matched = 1, scanned = 3, updated = 1, rowIDs = RowIDRange((0L to 3L).toList)))

      val device = scope.resolveTableVariable("travelers")
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 5, "firstName" -> "GARRY", "id" -> 0),
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 6, "firstName" -> "DEBBIE", "id" -> 1),
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 4, "firstName" -> "TAMERA", "id" -> 2),
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 7, "firstName" -> "ERIC", "id" -> 3)
      ))
    }

    it("should support execution using -=") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(),
        """|declare table travelers(
           |  id RowNumber,
           |  position Int,
           |  lastName String(32),
           |  firstName String(32),
           |  airportCode String(3))
           |
           |insert into @@travelers (position, lastName, firstName, airportCode)
           |values (5, 'JONES', 'GARRY', 'SNA'),
           |       (6, 'JONES', 'DEBBIE', 'SNA'),
           |       (8, 'JONES', 'TAMERA', 'SNA'),
           |       (7, 'JONES', 'ERIC', 'SNA') &&
           |
           |update @@travelers
           |set airportCode = 'LAX', position -= 4
           |where lastName is 'JONES' and firstName is 'TAMERA'
           |limit 1
           |""".stripMargin)
      assert(cost == IOCost(created = 1, inserted = 4, matched = 1, scanned = 3, updated = 1, rowIDs = RowIDRange((0L to 3L).toList)))

      val device = scope.resolveTableVariable("travelers")
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 5, "firstName" -> "GARRY", "id" -> 0),
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 6, "firstName" -> "DEBBIE", "id" -> 1),
        Map("airportCode" -> "LAX", "lastName" -> "JONES", "position" -> 4, "firstName" -> "TAMERA", "id" -> 2),
        Map("airportCode" -> "SNA", "lastName" -> "JONES", "position" -> 7, "firstName" -> "ERIC", "id" -> 3)
      ))
    }

  }

}
