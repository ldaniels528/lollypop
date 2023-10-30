package com.lollypop.runtime.devices

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.devices.CursorIterator.CursorIteratorRowCollection
import com.lollypop.runtime.errors.IteratorEmptyError
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.EQ
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CursorIteratorTest extends AnyFunSpec with VerificationTools {

  describe(classOf[CursorIterator].getSimpleName) {

    it("should return rows from the host device that match the condition") {
      implicit val scope: Scope = Scope()
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.cursor(where = Option(EQ("exchange".f, "OTCBB".v)))
      assert(cursor.toMapGraph == List(
        Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765)
      ))
    }

    it("should use previous() and next() to move backward/forward within the cursor") {
      implicit val scope: Scope = Scope()
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.cursor(where = Option(EQ("exchange".f, "NASDAQ".v)))

      // move forward through the cursor
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63))
      assert(!cursor.hasNext)
      assertThrows[IteratorEmptyError](cursor.next())

      // move backward through the cursor
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(!cursor.hasPrevious)
      assertThrows[IteratorEmptyError](cursor.previous())
    }

    it("should use peek() to get an option of the next row") {
      implicit val scope: Scope = Scope()
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.cursor(where = Option(EQ("exchange".f, "NASDAQ".v)))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(cursor.hasNext & cursor.peek.map(_.toMap).contains(Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01)))
    }

    it("should return all rows within the host device if the condition is empty") {
      implicit val scope: Scope = Scope()
      val ref = DatabaseObjectRef(getTestTableName)
      val device = createTestTable(ref)
      val cursor = device.cursor(where = None)

      // move forward through the cursor
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63))
      assert(cursor.hasNext & cursor.next().toMap == Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765))
      assert(!cursor.hasNext)

      // move backward through the cursor
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.50))
      assert(cursor.hasPrevious & cursor.previous().toMap == Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11))
      assert(!cursor.hasPrevious)

      // verify the graph
      assert(cursor.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 202.11),
        Map("symbol" -> "AMD", "exchange" -> "NYSE", "lastSale" -> 23.5),
        Map("symbol" -> "GOOG", "exchange" -> "NYSE", "lastSale" -> 765.33),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "lastSale" -> 699.01),
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92),
        Map("symbol" -> "GTKK", "exchange" -> "OTCBB", "lastSale" -> 4.63),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 0.765)
      ))

      // verify the previous state
      assert(!cursor.hasPrevious)
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
