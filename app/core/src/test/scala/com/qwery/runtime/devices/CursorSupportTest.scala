package com.qwery.runtime.devices

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CursorSupportTest extends AnyFunSpec with VerificationTools {

  describe(classOf[CursorSupport].getSimpleName) {

    it("should use previous() and next() to move backward/forward within the cursor") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = CursorSupport(device)

      // move forward through the cursor
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(!cursor.hasNext)

      // move backward through the cursor
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(!cursor.hasPrevious)
    }

    it("should use peek() to get an option of the next row") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = CursorSupport(device)
      assert(cursor.hasNext & cursor.peek.map(_.toMap).contains(Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11)))
    }

    it("should use seek() to move the cursor a specific position") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = CursorSupport(device)
      cursor.seek(3)
      assert(cursor.get.toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(!cursor.hasNext)
    }

  }

  private def createTestTable(ref: DatabaseObjectRef): RowCollection = {
    val (scope0, cost0, _) = QweryVM.executeSQL(Scope(),
      s"""|drop if exists $ref
          |create table $ref (
          |  symbol: String(8),
          |  exchange: String(8),
          |  lastSale: Double)
          |insert into $ref (symbol, exchange, lastSale)
          |values ('AAPL', 'NASDAQ', 202.11), ('AMD', 'NYSE', 23.50),
          |       ('GOOG', 'NYSE', 765.33), ('AMZN', 'NASDAQ', 699.01)
          |""".stripMargin)
    val device = scope0.getRowCollection(ref)
    assert(cost0.inserted == 4)
    device
  }

}
