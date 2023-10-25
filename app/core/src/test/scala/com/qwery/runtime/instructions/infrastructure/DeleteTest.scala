package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.devices.RowMetadata
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.conditions.{AND, WhereIn}
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import qwery.io.{IOCost, RowIDRange}

class DeleteTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Delete].getSimpleName) {

    it("should delete a row and confirm get conformation via its metadata") {
      // create a new table && insert some records
      val (scope0, cost0, _) = QweryVM.executeSQL(Scope(),
        """|drop if exists `tickers` &&
           |create table `tickers` (
           |    symbol: String(4),
           |    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |    lastSale: Double
           |) &&
           |insert into `tickers`
           ||------------------------------|
           || symbol | exchange | lastSale |
           ||------------------------------|
           || AAXX   | NYSE     |    56.12 |
           || UPEX   | OTHEROTC |   116.24 |
           || XYZ    | AMEX     |    31.95 |
           || JUNK   | AMEX     |    97.61 |
           || ABC    | OTCBB    |    5.887 |
           ||------------------------------|
           |order by symbol
           |""".stripMargin)
      assert(cost0 == IOCost(created = 1, destroyed = 1, matched = 5, scanned = 18, updated = 8, inserted = 5, rowIDs = RowIDRange(0L, 1, 2, 3, 4)))

      // confirm the data was inserted
      val (scope1, _, result1) = QweryVM.searchSQL(scope0,
        """|select * from `tickers`
           |""".stripMargin)
      assert(result1.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61),
        Map("symbol" -> "UPEX", "exchange" -> "OTHEROTC", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95)
      ))

      // delete a record
      val (scope2, cost2, _) = QweryVM.executeSQL(scope1,
        """|delete from `tickers` where symbol is 'AAXX'
           |""".stripMargin)
      assert(cost2 == IOCost(deleted = 1, matched = 1, scanned = 5))

      // verify the row metadata
      val (_, _, result3) = QweryVM.executeSQL(scope2,
        """|ns('tickers').readRowMetadata(0)
           |""".stripMargin)
      assert(result3 == RowMetadata(isAllocated = false))
    }

    it("should compile delete instructions referencing inner-tables") {
      val results = QweryCompiler().compile(
        """|delete from stocks#transactions
           |where symbol is 'SHMN'
           |and transactions wherein (price is 0.001)
           |limit 1
           |""".stripMargin)
      assert(results == Delete(
        ref = DatabaseObjectRef("stocks#transactions"),
        condition = AND("symbol".f is "SHMN", WhereIn("transactions".f, "price".f is 0.001.v)),
        limit = Some(1)
      ))
    }

    it("should decompile delete instructions referencing inner-tables") {
      val model = Delete(
        ref = DatabaseObjectRef("stocks#transactions"),
        condition = AND("symbol".f is "SHMN", WhereIn("transactions".f, "price".f is 0.001.v)),
        limit = Some(1))
      assert(model.toSQL ==
        """|delete from stocks#transactions
           |where (symbol is "SHMN")
           |and (transactions wherein price is 0.001)
           |limit 1
           |""".stripMargin.replaceAll("\n", " ").trim)
    }

    it("should delete rows from a clustered inner-table") {
      val ref = DatabaseObjectRef("stocks")
      val (scope, cost, _) = QweryVM.executeSQL(Scope(),
        s"""|drop if exists $ref &&
            |create table $ref (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (
            |       price Double,
            |       transactionTime DateTime
            |   )[5]
            |) &&
            |
            |insert into $ref (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMD', 'NASDAQ',  {"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]) &&
            |                          
            |delete from $ref#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin)
      assert(cost == IOCost(created = 1, destroyed = 1, deleted = 1, inserted = 5, matched = 2, scanned = 7, rowIDs = RowIDRange(0, 1, 2, 3, 4)))
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
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

  }

}
