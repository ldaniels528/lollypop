package com.lollypop.runtime.devices

import com.lollypop.language.models.@@
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import lollypop.lang.BitArray

class MultiTenantRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[MultiTenantRowCollection].getSimpleName) {

    it("should retrieve only rows that belong to it and insert new rows into the host collection") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // get the source collection
      val source = scope0.getRowCollection(@@("stocks"))
      assert(source.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))

      // setup the view collection
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0, 1, 3, 7))
      val viewB = MultiTenantRowCollection(source, visibility = BitArray(2, 4, 5, 6))

      // put the resources in the scope
      val scope1 = scope0
        .withVariable("viewA", Some(viewA), isReadOnly = true)
        .withVariable("viewB", Some(viewB), isReadOnly = true)

      // insert a new row
      val (_, cost, _) = LollypopVM.executeSQL(scope1,
        """|insert into @viewA (symbol, exchange, lastSale) values ("PMP", "AMEX", 137.80), ("TNT", "NASDAQ", 45.11) &&
           |insert into @viewB (symbol, exchange, lastSale) values ("XL", "OTCBB", 1.80), ("GMS", "NYSE", 123.45)
           |""".stripMargin)
      assert(cost.inserted == 4)

      // get the source collection
      logger.info(s"Resource: ${source.getClass.getSimpleName}")
      source.tabulate().foreach(logger.info)

      // verify the results of view A
      logger.info("")
      logger.info(s"View A: ${viewA.getClass.getSimpleName}")
      viewA.tabulate().foreach(logger.info)
      assert(viewA.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99),
        Map("symbol" -> "PMP", "exchange" -> "AMEX", "lastSale" -> 137.80),
        Map("symbol" -> "TNT", "exchange" -> "NASDAQ", "lastSale" -> 45.11)
      ))

      // verify the results of view B
      logger.info("")
      logger.info(s"View B: ${viewB.getClass.getSimpleName}")
      viewB.tabulate().foreach(logger.info)
      assert(viewB.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "XL", "exchange" -> "OTCBB", "lastSale" -> 1.80),
        Map("symbol" -> "GMS", "exchange" -> "NYSE", "lastSale" -> 123.45)
      ))
    }

    it("should remove only rows that belong to it") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // put the resources in the scope
      val source = scope0.getRowCollection(@@("stocks"))
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0, 1, 3, 7))
      val viewB = MultiTenantRowCollection(source, visibility = BitArray(2, 4, 5, 6))
      val scope1 = scope0
        .withVariable("viewA", Some(viewA), isReadOnly = true)
        .withVariable("viewB", Some(viewB), isReadOnly = true)

      LollypopVM.executeSQL(scope1,
        """|delete from @viewA where symbol is "UPEX"
           |delete from @viewA where symbol is "XYZ"
           |delete from @viewB where symbol is "ZZY"
           |delete from @viewB where symbol is "ABC"
           |""".stripMargin)

      // verify the results of view A
      logger.info("")
      logger.info(s"View A: ${viewA.getClass.getSimpleName}")
      viewA.tabulate().foreach(logger.info)
      assert(viewA.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))

      // verify the results of view B
      logger.info("")
      logger.info(s"View B: ${viewB.getClass.getSimpleName}")
      viewB.tabulate().foreach(logger.info)
      assert(viewB.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001)
      ))
    }

    it("should read fields from rows that belong to it") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // put the resources in the scope
      val source = scope0.getRowCollection(@@("stocks"))
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0, 1, 3, 7))
      assert(viewA.readField(rowID = 2, columnID = 0).value.isEmpty)
      assert(viewA.readField(rowID = 3, columnID = 0).value contains "ABC")
    }

    it("should update fields from rows that belong to it") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // put the resources in the scope
      val source = scope0.getRowCollection(@@("stocks"))
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0, 1, 3, 7))
      assert(viewA.updateField(rowID = 2, columnID = 0, newValue = Some("DUMMY")).updated == 0)
      assert(viewA.updateField(rowID = 3, columnID = 0, newValue = Some("JUNK")).updated == 1)
      assert(viewA.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "JUNK", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
    }

    it("should update rows that belong to it") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // put the resources in the scope
      val source = scope0.getRowCollection(@@("stocks"))
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0, 1, 3, 7))
      LollypopVM.executeSQL(scope0.withVariable("viewA", Some(viewA), isReadOnly = true),
        """|update @viewA set lastSale = 14.07 where symbol is "GABY"
           |""".stripMargin)
      assert(viewA.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 14.07)
      ))
    }

    it("should truncate rows that belong to it") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        """|val stocks = Table(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @stocks
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | ABC    | OTCBB    |    5.887 |
           |    | TRIX   | NYSE     |    77.88 |
           |    | ZZZ    | OTCBB    |   0.0001 |
           |    | ZZY    | OTCBB    |   0.0011 |
           |    | GABY   | NASDAQ   |    13.99 |
           |    |------------------------------|
           |""".stripMargin)

      // put the resources in the scope
      val source = scope0.getRowCollection(@@("stocks"))
      val viewA = MultiTenantRowCollection(source, visibility = BitArray(0L until source.getLength: _*))
      assert(viewA.setLength(1).deleted == 7)
      assert(viewA.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12)
      ))
    }

  }

}
