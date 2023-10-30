package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class UnDeleteTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[UnDelete].getSimpleName) {

    it("should support conversion from SQL to model") {
      val model = compiler.compile("undelete from todo_list where item_id is 1238 limit 25")
      assert(model == UnDelete(ref = DatabaseObjectRef("todo_list"), condition = "item_id".f is 1238, limit = Some(25)))
    }

    it("should support conversion from model to SQL") {
      val model = UnDelete(ref = DatabaseObjectRef("todo_list"), condition = "item_id".f is 1238, limit = Some(25))
      assert(model.toSQL == "undelete from todo_list where item_id is 1238 limit 25")
    }

    it("should restore deleted rows") {
      val ref = DatabaseObjectRef(getTestTableName)
      // create a table and insert some data
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(), sql =
        s"""|drop if exists $ref &&
            |create table $ref (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (
            |     price Double,
            |     transactionTime DateTime
            |   )
            |) &&
            |insert into $ref (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {"price":156.39, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('AMD', 'NASDAQ', {"price":56.87, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('INTC','NYSE', {"price":89.44, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-06T15:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-06T15:23:11.000Z"},
            |                          {"price":0.0011, "transactionTime":"2021-08-06T15:23:12.000Z"}])
            |""".stripMargin)
      assert((cost0.created contains 1) && (cost0.inserted == 5))

      // delete a row
      val (scope1, cost1, _) = LollypopVM.executeSQL(scope0, sql =
        s"""|delete from $ref where symbol is "AAPL"
            |""".stripMargin)
      assert(cost1.deleted == 1)

      // verify that the row was removed
      assert(scope1.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-06T15:23:12.000Z"))))
      ))

      // restore the deleted row
      val (scope2, cost2, _) = LollypopVM.executeSQL(scope1, sql =
        s"""|undelete from $ref where symbol is "AAPL"
            |""".stripMargin)
      assert(cost2.updated == 1)

      // verify the deleted row was restored
      assert(scope2.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 156.39, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")))),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-06T15:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-06T15:23:12.000Z"))))
      ))
    }

    it("should restore rows from a clustered inner-table") {
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
            |delete from $ref#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin)
      assert(cost.deleted == 1)
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

      // restore the deleted inner row
      val (_, cost2, _) = LollypopVM.executeSQL(scope, sql =
        s"""|undelete from $ref#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin)
      assert(cost2.updated == 1)

      // verify the row was restored
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
          Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

  }

}
