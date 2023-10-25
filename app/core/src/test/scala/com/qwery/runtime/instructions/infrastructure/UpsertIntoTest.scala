package com.qwery.runtime.instructions.infrastructure

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class UpsertIntoTest extends AnyFunSpec with VerificationTools {
  private val ref = DatabaseObjectRef(getTestTableName)

  describe(classOf[UpsertInto].getSimpleName) {

    it("should insert/update rows from values") {
      val (scope0, cost0, _) = QweryVM.executeSQL(Scope(),
        s"""|drop if exists $ref &&
            |create table $ref (symbol: String(8), exchange: String(8), lastSale: Double) &&
            |create index $ref#symbol &&
            |insert into $ref (symbol, exchange, lastSale)
            |  |------------------------------|
            |  | symbol | exchange | lastSale |
            |  |------------------------------|
            |  | AAXX   | NYSE     |    56.12 |
            |  | UPEX   | NASDAQ   |   116.24 |
            |  | XYZ    | AMEX     |    31.95 |
            |  | ABC    | OTCBB    |    5.887 |
            |  |------------------------------|
            |""".stripMargin)
      assert(cost0.inserted == 4)

      val (scope1, cost1, _) = QweryVM.executeSQL(scope0.reset(),
        s"""|upsert into $ref (symbol, exchange, lastSale)
            |values ('AAPL', 'NASDAQ', 156.39)
            |where symbol is 'AAPL'
            |""".stripMargin)
      assert(cost1.scanned == 0 && cost1.inserted == 1 && cost1.updated == 0)

      val (scope2, cost2, _) = QweryVM.executeSQL(scope1.reset(),
        s"""|upsert into $ref (symbol, exchange, lastSale)
            |values ('AAPL', 'NASDAQ', 82.33)
            |where symbol is 'AAPL'
            |""".stripMargin)
      assert(cost1.scanned == 0 && cost2.inserted == 0 && cost2.updated == 1)

      val device = scope2.getRowCollection(ref)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NASDAQ", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 82.33)
      ))
    }

  }

}
