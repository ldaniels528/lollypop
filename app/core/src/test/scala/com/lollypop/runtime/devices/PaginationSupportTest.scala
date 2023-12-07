package com.lollypop.runtime.devices

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class PaginationSupportTest extends AnyFunSpec with VerificationTools {

  describe(classOf[PaginationSupport].getSimpleName) {
    val ref = DatabaseObjectRef(getTestTableName)

    it("should support pagination on a collection") {
      val device = createTestTable(ref)
      val cursor = PaginationSupport(device)

      // verify the first 2
      assert(cursor.first(limit = 2).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11),
        Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.5)
      ))

      // verify the next 3
      assert(cursor.next(limit = 3).toMapGraph == List(
        Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01),
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63)
      ))

      // verify the last 3
      assert(cursor.last(limit = 3).toMapGraph == List(
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92),
        Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765)
      ))
      assert(!cursor.hasNext)

      // verify position 4
      cursor.seek(4)
      assert(cursor.current.toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63))

      // verify the previous 2
      assert(cursor.previous(limit = 2).toMapGraph == List(
        Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01)
      ))

      // no previous at position 0
      cursor.seek(0)
      assert(!cursor.hasPrevious)

      // verify the last 3
      assert(cursor.last(limit = 3).toMapGraph == List(
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92),
        Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765)
      ))
    }

  }

  private def createTestTable(ref: DatabaseObjectRef): RowCollection = {
    val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(),
      s"""|drop if exists $ref
          |create table $ref (
          |  symbol: String(8),
          |  exchange: String(8),
          |  lastSale: Double)
          |insert into $ref (symbol, exchange, lastSale)
          |values ('AAPL', 'NASDAQ', 202.11), ('AMD', 'NYSE', 23.50),
          |       ('GOOG', 'NYSE', 765.33), ('AMZN', 'NASDAQ', 699.01),
          |       ('BXXG', 'NASDAQ', 147.63), ('KFFQ', 'NYSE', 22.92),
          |       ('GTKK', 'OTCBB', 4.63), ('KNOW', 'OTCBB', 0.765)
          |""".stripMargin)
    val device = scope0.getRowCollection(ref)
    assert(cost0.inserted == 8)
    device
  }

}
