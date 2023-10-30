package com.lollypop.runtime.devices

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality._
import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.ResourceHelper.AutoClose
import com.lollypop.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import scala.io.Source

/**
 * Row Collection Test Suite
 * @author lawrence.daniels@gmail.com
 */
class RowCollectionTest extends AnyFunSpec with VerificationTools {
  implicit val scope: Scope = Scope()
  private val columnsA_B = Seq(
    Column(name = "symbol", `type` = ColumnType("String", size = 8)),
    Column(name = "exchange", `type` = ColumnType("String", size = 8)),
    Column(name = "lastSale", `type` = ColumnType("Double")),
    Column(name = "lastSaleTime", `type` = ColumnType("Long"))
  ).map(_.toTableColumn)
  private val dataSetA = List[Map[String, Any]](
    Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
    Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
    Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
    Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
  )
  private val dataSetB = List[Map[String, Any]](
    Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
    Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
    Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
    Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
    Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
    Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
  )
  private val columnsC = Seq(
    Column(name = "rating", `type` = ColumnType("Int")),
    Column(name = "category", `type` = ColumnType("String", size = 8)),
  ).map(_.toTableColumn)
  private val dataSetC = List[Map[String, Any]](
    Map("rating" -> 3, "category" -> "non-ETF")
  )

  describe(classOf[RowCollection].getSimpleName) {

    it("should use + (plus) to combine two collections") {
      val collA = newTableA()
      val collB = newTableB()
      assert((collA + collB).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use - (minus) to subtract a portion of two collections") {
      val collA = newTableA()
      val collB = newTableB()
      assert((collA - collB).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use | (bar) to create a distinct combined set from two collections") {
      val collA = newTableA()
      val collB = newTableB()
      assert((collA | collB).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use & (amp) to determine the intersection of two collections") {
      val collA = newTableA()
      val collB = newTableB()
      assert((collA & collB).toMapGraph == List(
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L)
      ))
    }

    it("should use * (times) to product the product of two collections") {
      val collA = newTableA()
      val collC = newTableC()
      assert((collA * collC).toMapGraph == List(
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "rating" -> 3, "lastSale" -> 147.63, "category" -> "non-ETF", "lastSaleTime" -> 1596317591000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "rating" -> 3, "lastSale" -> 22.92, "category" -> "non-ETF", "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "rating" -> 3, "lastSale" -> 240.14, "category" -> "non-ETF", "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "rating" -> 3, "lastSale" -> 357.21, "category" -> "non-ETF", "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use / (div) to partition a collection by column") {
      val collA = newTableA() | newTableB()
      val collB = collA / Array("exchange")

      // verify the partitions
      val partitionRC = collB match {
        case rc: PartitionedRowCollection[_] => rc
        case x => fail(s"${x.render} is a not PartitionedRowCollection")
      }
      assert(partitionRC.getPartitionMap.map { case (key, rc) => key -> rc.toMapGraph } == Map(
        "NYSE" -> List(
          Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
          Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L)
        ),
        "NASDAQ" -> List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
          Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
          Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L)
        ),
        "OTCBB" -> List(
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
          Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
        )
      ))
    }

    it("should use % (percent) to extract a columnar slice of a table") {
      val collA = newTableA()
      val columns = Array("symbol", "exchange")
      assert((collA % columns).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ"),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE"),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ"),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB")
      ))
    }

    it("should execute + (plus) to combine two collections") {
      val collA = newTableA()
      val collB = newTableB()
      val (_, _, results) = LollypopVM.searchSQL(Scope().withVariable("collA", collA).withVariable("collB", collB),
        """|collA + collB
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should execute - (minus) to subtract a portion of two collections") {
      val collA = newTableA()
      val collB = newTableB()
      val (_, _, results) = LollypopVM.searchSQL(Scope().withVariable("collA", collA).withVariable("collB", collB),
        """|collA - collB
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should execute | (bar) to create a distinct combined set from two collections") {
      val collA = newTableA()
      val collB = newTableB()
      val (_, _, results) = LollypopVM.searchSQL(Scope().withVariable("collA", collA).withVariable("collB", collB),
        """|collA | collB
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should execute & (amp) to determine the intersection of two collections") {
      val collA = newTableA()
      val collB = newTableB()
      val (_, _, results) = LollypopVM.searchSQL(Scope().withVariable("collA", collA).withVariable("collB", collB),
        """|collA & collB
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L)
      ))
    }

    it("should execute * (times) to product the product of two collections") {
      val collA = newTableA()
      val collC = newTableC()
      val (_, _, results) = LollypopVM.searchSQL(Scope().withVariable("collA", collA).withVariable("collC", collC),
        """|collA * collC
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "rating" -> 3, "lastSale" -> 147.63, "category" -> "non-ETF", "lastSaleTime" -> 1596317591000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "rating" -> 3, "lastSale" -> 22.92, "category" -> "non-ETF", "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "rating" -> 3, "lastSale" -> 240.14, "category" -> "non-ETF", "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "rating" -> 3, "lastSale" -> 357.21, "category" -> "non-ETF", "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should execute / (div) to partition a collection by column") {
      val coll = newTableA() | newTableB()
      val (_, _, device) = LollypopVM.searchSQL(Scope().withVariable("stocks", coll), "stocks / ['exchange']")

      // verify the partitions
      val partitionRC = device match {
        case rc: PartitionedRowCollection[_] => rc
        case x => fail(s"${x.render} is a not PartitionedRowCollection")
      }
      assert(partitionRC.getPartitionMap.map { case (key, rc) => key -> rc.toMapGraph } == Map(
        "NYSE" -> List(
          Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
          Map("symbol" -> "LMRE", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L)
        ),
        "NASDAQ" -> List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
          Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
          Map("symbol" -> "XVII", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L)
        ),
        "OTCBB" -> List(
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
          Map("symbol" -> "TDWA", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
        )
      ))
    }

    it("should execute % (percent) to extract a columnar slice of a table") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|val stocks: Table(symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)[5000] =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |stocks % ['exchange']
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("exchange" -> "NYSE"),
        Map("exchange" -> "NYSE"),
        Map("exchange" -> "AMEX"),
        Map("exchange" -> "OTCBB")
      ))
    }

    it("should use .countWhere() to count the rows where: `exchange` is 'NASDAQ')") {
      val cost = newTableA().countWhere(Some("exchange".f === "NASDAQ".v))
      assert(cost.matched == 2)
    }

    it("should use .deleteWhere() to remove the rows where: `exchange` is 'NASDAQ')") {
      val device = newTableA()
      val cost = device.deleteWhere(Some("exchange".f === "NASDAQ".v), limit = None)
      assert(cost.matched == 2)
      assert(device.toMapGraph == List(
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use .distinct to filter out duplicates") {
      implicit val rc: RowCollection = ByteArrayRowCollection(columnsA_B, capacity = dataSetA.length * 2)
      (dataSetA ::: dataSetA).foreach(row => rc.insert(row.toRow))
      assert(rc.distinct.toMapGraph == List(
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should use .indexOf() to determine the index of a row in the table") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|declare table stocks(symbol: String(6), exchange: String(6), lastSale: Double, groupCode: Char)
           |containing values
           |   ('AAXX', 'NYSE', 56.12, 'A'), ('UPEX', 'NYSE', 116.24, 'A'), ('XYZ', 'AMEX',   31.9500, 'A'),
           |   ('JUNK', 'AMEX', 97.61, 'B'), ('RTX',  'OTCBB', 1.9301, 'B'), ('ABC', 'NYSE', 1235.7650, 'B'),
           |   ('UNIB', 'OTCBB',  9.11, 'C'), ('BRT.OB', 'OTCBB', 0.0012, 'C'), ('PLUMB', 'NYSE',  809.0770, 'C')
           |
           |stocks.indexOf({ symbol: 'UPEX' })
           |""".stripMargin
      )
      assert(result == 1)
    }

    it("should support push/pop mechanics") {
      val (_, _, results) = LollypopVM.executeSQL(Scope(),
        """|declare table stocks(symbol: String(5), exchange: String(6), lastSale: Double)
           |stocks.push({ symbol: 'ABC', exchange: 'OTCBB', lastSale: 37.89 })
           |stocks.pop()
           |""".stripMargin)
      val mapping = Option(results).collect { case r: Row => r.toMap }
      assert(mapping contains Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 37.89))
    }

    it("should push rows") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|declare table stocks(symbol: String(5), exchange: String(6), lastSale: Double)
           |stocks.push({ symbol: 'ABC', exchange: 'OTCBB', lastSale: 37.89 })
           |stocks.push({ symbol: 'T', exchange: 'NYSE', lastSale: 22.77 })
           |stocks.push({ symbol: 'AAPL', exchange: 'NASDAQ', lastSale: 149.76 })
           |stocks
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 37.89),
        Map("symbol" -> "T", "exchange" -> "NYSE", "lastSale" -> 22.77),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 149.76)
      ))
    }

    it("should remove a row from a collection") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|val stocks: Table(symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)[5] =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |stocks.remove(3)
           |stocks
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95)
      ))
    }

    it("should use .reverse() to reverse the rows of the collection") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|declare table stocks(symbol: String(7), exchange: String(6), lastSale: Double, groupCode: Char)
           | containing values
           |    ('AAXX', 'NYSE', 56.12, 'A'), ('UPEX', 'NYSE', 116.24, 'A'), ('XYZ', 'AMEX',   31.9500, 'A'),
           |    ('JUNK', 'AMEX', 97.61, 'B'), ('RTX.OB',  'OTCBB', 1.93011, 'B'), ('ABC', 'NYSE', 1235.7650, 'B'),
           |    ('UNIB.OB', 'OTCBB',  9.11, 'C'), ('BRT.OB', 'OTCBB', 0.00123, 'C'), ('PLUMB', 'NYSE',  809.0770, 'C')
           |
           |stocks.reverse()
           |""".stripMargin
      )
      assert(device.toMapGraph == List(
        Map("exchange" -> "NYSE", "symbol" -> "PLUMB", "groupCode" -> 'C', "lastSale" -> 809.077),
        Map("exchange" -> "OTCBB", "symbol" -> "BRT.OB", "groupCode" -> 'C', "lastSale" -> 0.00123),
        Map("exchange" -> "OTCBB", "symbol" -> "UNIB.OB", "groupCode" -> 'C', "lastSale" -> 9.11),
        Map("exchange" -> "NYSE", "symbol" -> "ABC", "groupCode" -> 'B', "lastSale" -> 1235.765),
        Map("exchange" -> "OTCBB", "symbol" -> "RTX.OB", "groupCode" -> 'B', "lastSale" -> 1.93011),
        Map("exchange" -> "AMEX", "symbol" -> "JUNK", "groupCode" -> 'B', "lastSale" -> 97.61),
        Map("exchange" -> "AMEX", "symbol" -> "XYZ", "groupCode" -> 'A', "lastSale" -> 31.95),
        Map("exchange" -> "NYSE", "symbol" -> "UPEX", "groupCode" -> 'A', "lastSale" -> 116.24),
        Map("exchange" -> "NYSE", "symbol" -> "AAXX", "groupCode" -> 'A', "lastSale" -> 56.12)
      ))
    }

    it("should combines two result sets horizontally") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|val stocks =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |
           |val companyNames =
           |    |------------------------------|
           |    | name                         |
           |    |------------------------------|
           |    | 'Apex Extreme'               |
           |    | 'United Pirates'             |
           |    | 'XYZ Co.'                    |
           |    | 'Junky Corporation'          |
           |    | 'Another Bad Creation'       |
           |    |------------------------------|
           |
           |stocks.zipRows(companyNames)
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "name" -> "Apex Extreme"),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24, "name" -> "United Pirates"),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95, "name" -> "XYZ Co."),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61, "name" -> "Junky Corporation"),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887, "name" -> "Another Bad Creation")
      ))
    }

    it("should write data to a file in CSV format") {
      val (_, _, tempFile) = LollypopVM.executeSQL(Scope(),
        s"""|val passengers = Table(lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @@passengers (lastName, firstName, destAirportCode)
            |values
            |   ('JONES', 'GARRY', 'SNA'), ('JONES', 'DEBBIE', 'SNA'),
            |   ('JONES', 'TAMERA', 'SNA'), ('JONES', 'ERIC', 'SNA'),
            |   ('ADAMS', 'KAREN', 'DTW'), ('ADAMS', 'MIKE', 'DTW'),
            |   ('JONES', 'SAMANTHA', 'BUR'), ('SHARMA', 'PANKAJ', 'LAX')
            |
            |import 'java.io.File'
            |val tempFile = File.createTempFile("test", ".csv")
            |passengers.exportAsCSV(tempFile)
            |tempFile
            |""".stripMargin)
      val contents = Option(tempFile).collect { case file: File => Source.fromFile(file).use(_.mkString) }
      assert(contents contains
        """|"lastName","firstName","destAirportCode"
           |"JONES","GARRY","SNA"
           |"JONES","DEBBIE","SNA"
           |"JONES","TAMERA","SNA"
           |"JONES","ERIC","SNA"
           |"ADAMS","KAREN","DTW"
           |"ADAMS","MIKE","DTW"
           |"JONES","SAMANTHA","BUR"
           |"SHARMA","PANKAJ","LAX"
           |""".stripMargin)
    }

    it("should write data to a file in JSON format") {
      val (_, _, tempFile) = LollypopVM.executeSQL(Scope(),
        s"""|val passengers = Table(lastName String(12), firstName String(12), destAirportCode String(3))
            |insert into @@passengers(lastName, firstName, destAirportCode)
            |values
            |   ('JONES', 'GARRY', 'SNA'), ('JONES', 'DEBBIE', 'SNA'),
            |   ('JONES', 'TAMERA', 'SNA'), ('JONES', 'ERIC', 'SNA'),
            |   ('ADAMS', 'KAREN', 'DTW'), ('ADAMS', 'MIKE', 'DTW'),
            |   ('JONES', 'SAMANTHA', 'BUR'), ('SHARMA', 'PANKAJ', 'LAX')
            |
            |import 'java.io.File'
            |val tempFile = File.createTempFile("test", ".json")
            |passengers.exportAsJSON(tempFile)
            |tempFile
            |""".stripMargin)
      val contents = Option(tempFile).collect { case file: File => Source.fromFile(file).use(_.mkString) }
      assert(contents contains
        """|{ "lastName":"JONES","firstName":"GARRY","destAirportCode":"SNA" }
           |{ "lastName":"JONES","firstName":"DEBBIE","destAirportCode":"SNA" }
           |{ "lastName":"JONES","firstName":"TAMERA","destAirportCode":"SNA" }
           |{ "lastName":"JONES","firstName":"ERIC","destAirportCode":"SNA" }
           |{ "lastName":"ADAMS","firstName":"KAREN","destAirportCode":"DTW" }
           |{ "lastName":"ADAMS","firstName":"MIKE","destAirportCode":"DTW" }
           |{ "lastName":"JONES","firstName":"SAMANTHA","destAirportCode":"BUR" }
           |{ "lastName":"SHARMA","firstName":"PANKAJ","destAirportCode":"LAX" }
           |""".stripMargin)
    }

  }

  private def newTableA(): RowCollection = newTable(columnsA_B, dataSetA)

  private def newTableB(): RowCollection = newTable(columnsA_B, dataSetB)

  private def newTableC(): RowCollection = newTable(columnsC, dataSetC)

  private def newTable(columns: Seq[TableColumn], data: List[Map[String, Any]]): RowCollection = {
    implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = data.length)
    data.foreach(row => device.insert(row.toRow))
    device
  }

}
