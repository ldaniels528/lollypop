package com.lollypop.runtime.instructions.conditions

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class WhereInTest extends AnyFunSpec with VerificationTools {

  describe(classOf[WhereIn].getSimpleName) {

    it(s"should match rows within an embedded durable table") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (_, _, result) = LollypopVM.searchSQL(Scope(), sql =
        s"""|drop if exists $ref
            |create table $ref(
            |   symbol: String(4),
            |   exchange: String(8),
            |   transactions Table (price Double, transactionTime DateTime)[2]
            |)
            |
            |insert into $ref (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {price: 156.39, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('AMD',  'NASDAQ', {price:  56.87, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('INTC', 'NYSE',   {price:  89.44, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {price: 988.12, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{price: 0.0010, transactionTime: "2021-08-05T19:23:11.000Z"},
            |                          {price: 0.0011, transactionTime: "2021-08-05T19:23:12.000Z"}])
            |
            |select * from $ref
            |where transactions wherein (price is 0.0010)
            |""".stripMargin)
      assert(result.toMapGraph == List(
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it(s"should match rows within an embedded ephemeral table") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(), sql =
        s"""|val stocks = Table(
            |   symbol: String(4),
            |   exchange: String(8),
            |   transactions Table (price Double, transactionTime DateTime)[2]
            |)
            |
            |insert into @@stocks (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', {price: 156.39, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('AMD',  'NASDAQ', {price:  56.87, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('INTC', 'NYSE',   {price:  89.44, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('AMZN', 'NASDAQ', {price: 988.12, transactionTime: "2021-08-05T19:23:11.000Z"}),
            |       ('SHMN', 'OTCBB', [{price: 0.0010, transactionTime: "2021-08-05T19:23:11.000Z"},
            |                          {price: 0.0011, transactionTime: "2021-08-05T19:23:12.000Z"}])
            |
            |select unnest(transactions)
            |from @@stocks
            |where transactions wherein (price is 0.0011)
            |""".stripMargin)
      assert(result.toMapGraph == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

  }

}
