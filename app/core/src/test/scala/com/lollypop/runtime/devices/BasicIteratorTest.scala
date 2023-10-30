package com.lollypop.runtime.devices

import com.lollypop.runtime.devices.BasicIterator.RichBasicIterator
import com.lollypop.runtime.errors.IteratorEmptyError
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class BasicIteratorTest extends AnyFunSpec with VerificationTools {

  describe(classOf[BasicIterator[Row]].getSimpleName) {

    it("should move forward through the iterator") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.iterator
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(!cursor.hasNext)
      assertThrows[IteratorEmptyError](cursor.next())
    }

    it("should move in reverse through the iterator") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.reverseIterator
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(!cursor.hasNext)
      assertThrows[IteratorEmptyError](cursor.next())
    }

    it("should use peek() to get an option of the next row") {
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.iterator
      assert(cursor.hasNext & cursor.peek.map(_.toMap).contains(Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11)))
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
          |       ('GOOG', 'NYSE', 765.33), ('AMZN', 'NASDAQ', 699.01)
          |""".stripMargin)
    val device = scope0.getRowCollection(ref)
    assert(cost0.inserted == 4)
    device
  }

}
