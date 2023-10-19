package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class UnNestTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[UnNest].getSimpleName) {

    it("should compile SQL to a model") {
      val model = QweryCompiler().compile("unnest(transactions)")
      assert(model == UnNest("transactions".f))
    }

    it("should decompile a model to SQL") {
      val model = UnNest("transactions".f)
      assert(model.toSQL == "unnest(transactions)")
    }

    it("should UnNest nested tables") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        s"""|declare table stocks(
            |   symbol: String(4),
            |   exchange: String(6),
            |   transactions Table (
            |      price Double,
            |      transactionTime DateTime
            |   )[5]
            |)
            |
            |insert into @@stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {price:156.39, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('AMD',  'NASDAQ', {price:56.87, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('INTC', 'NYSE',   {price:89.44, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {price:988.12, transactionTime:"2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{price:0.0010, transactionTime:"2021-08-05T19:23:11.000Z"},
            |                          {price:0.0011, transactionTime:"2021-08-05T19:23:12.000Z"}])
            |
            |select symbol, exchange, unnest(transactions)
            |from @@stocks
            |where symbol is 'SHMN'
            |""".stripMargin
      )
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("price" -> 0.001, "exchange" -> "OTCBB", "symbol" -> "SHMN", "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "exchange" -> "OTCBB", "symbol" -> "SHMN", "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

    it("should unnest nested tables (JSON)") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        s"""|declare table stocks(
            |   symbol: String(4),
            |   exchange: String(6),
            |   transactions Table (
            |      price Double,
            |      transactionTime DateTime
            |   )[5]
            |)
            |
            |insert into @@stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', '{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMD',  'NASDAQ', '{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('INTC', 'NYSE',   '{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', '{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB',  '[{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                            {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]')
            |
            |select symbol, exchange, unnest(transactions)
            |from @@stocks
            |where symbol is 'SHMN'
            |""".stripMargin
      )
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("price" -> 0.001, "exchange" -> "OTCBB", "symbol" -> "SHMN", "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "exchange" -> "OTCBB", "symbol" -> "SHMN", "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

  }

}
