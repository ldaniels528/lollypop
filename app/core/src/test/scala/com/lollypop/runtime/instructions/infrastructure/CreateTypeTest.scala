package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import lollypop.io.{IOCost, RowIDRange}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CreateTypeTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateType].getSimpleName) {

    it("should support compiling create type .. as CLOB") {
      val results = compiler.compile(
        """|create type if not exists clob8K
           |as CLOB(8192)
           |""".stripMargin)
      assert(results == CreateType(ref = DatabaseObjectRef("clob8K"), userType = "CLOB".ct(size = 8192), ifNotExists = true))
    }

    it("should support compiling create type .. as ENUM") {
      val results = compiler.compile(
        """|create type mood
           |as Enum (sad, okay, happy)
           |""".stripMargin)
      assert(results == CreateType(ref = DatabaseObjectRef("mood"),
        userType = ColumnType.enum(enumValues = Seq("sad", "okay", "happy")),
        ifNotExists = false))
    }

    it("should support compiling create type .. as table") {
      val results = compiler.compile(
        """|create type priceHistory
           |as Table (price Double, updatedTime DateTime)
           |""".stripMargin)
      assert(results == CreateType(ref = DatabaseObjectRef("priceHistory"),
          userType = ColumnType.table(columns = Seq(
            Column(name = "price", "Double".ct),
            Column(name = "updatedTime", "DateTime".ct)
          )), ifNotExists = false))
    }

    it("should support custom types (CurrencyType)") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|create type Currency as UDT('com.github.ldaniels528.lollypop.CurrencyType')
           |val Currency = ns('Currency')
           |set value = Currency('$12.5M')
           |select value
           |""".stripMargin)
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(Map("value" -> 1.25e7)))
    }

    it("should support decompiling create type .. as CLOB") {
      verify(
        """|create type if not exists clob8K as CLOB(8192)
           |""".stripMargin)
    }

    it("should support decompiling create type .. as ENUM") {
      verify(
        """|create type mood as Enum (sad, okay, happy)
           |""".stripMargin)
    }

    it("should support decompiling create type .. as table") {
      verify(
        """|create type priceHistory as Table (price Double, updatedTime DateTime)
           |""".stripMargin)
    }

    it(s"should support create table with custom Type") {
      val ref = DatabaseObjectRef(getClass.getSimpleName)

      // create a type
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(), sql =
        s"""|namespace 'samples.stocks'
            |drop if exists Transactions &&
            |create type Transactions as Table (
            |   price Double,
            |   transactionTime DateTime
            |)[100]
            |""".stripMargin)
      assert(cost0 == IOCost(destroyed = 1, created = 1))

      // create a table using our type
      val (scope1, cost1, _) = LollypopVM.executeSQL(scope0, sql =
        s"""|drop if exists $ref &&
            |create table $ref (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Transactions
            |) &&
            |insert into $ref (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('AMD', 'NASDAQ', {"price":56.87, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('INTC','NYSE', {"price":89.44, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-06T15:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-06T15:23:12.000Z"}])
            |""".stripMargin)
      assert(cost1 == IOCost(destroyed = 1, created = 1, inserted = 5, rowIDs = RowIDRange(0L, 1, 2, 3, 4)))

      // read rows from our new table
      val (scope2, _, device2) = LollypopVM.searchSQL(scope1, sql =
        s"""|select symbol, exchange, transactions from $ref order by symbol asc
            |""".stripMargin)
      assert(device2.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 156.39, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-06T15:23:12.000Z"))))
      ))

      // convert the nested rows to JSON
      val (_, _, device3) = LollypopVM.searchSQL(scope2, sql =
        s"""select symbol, exchange, transactions: transactions.toJsonString()
           |from $ref
           |order by symbol asc
           |""".stripMargin)
      assert(device3.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> """[{"price":156.39,"transactionTime":"2021-08-06T15:23:11.000Z"}]"""),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> """[{"price":56.87,"transactionTime":"2021-08-06T15:23:11.000Z"}]"""),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> """[{"price":988.12,"transactionTime":"2021-08-06T15:23:11.000Z"}]"""),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> """[{"price":89.44,"transactionTime":"2021-08-06T15:23:11.000Z"}]"""),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> """[{"price":0.001,"transactionTime":"2021-08-06T15:23:11.000Z"},{"price":0.0011,"transactionTime":"2021-08-06T15:23:12.000Z"}]""")
      ))
    }


  }

}
