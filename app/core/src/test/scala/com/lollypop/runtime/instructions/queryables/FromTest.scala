package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.Date

class FromTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ctx = LollypopUniverse(isServerMode = true)

  describe(classOf[From].getSimpleName) {

    it("should extract rows from an array of objects") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|from [
           |    { item: "Apple" },
           |    { item: "Orange" },
           |    { item: "Cherry" }
           |]
           |limit 1
           |""".stripMargin)
      assert(device.toMapGraph == List(Map("item" -> "Apple")))
    }

    it("should extract rows from a table literal") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|from
           |    |-------------------------------------------------------------------------|
           |    | ticker | market | lastSale | roundedLastSale | lastSaleTime             |
           |    |-------------------------------------------------------------------------|
           |    | NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
           |    | AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
           |    | WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
           |    | ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
           |    | NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
           |    |-------------------------------------------------------------------------|
           |where market is 'AMEX'
           |order by lastSale desc
           |limit 2
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("market" -> "AMEX", "roundedLastSale" -> 46.8, "lastSale" -> 46.8355, "lastSaleTime" -> dateOf("2022-09-04T23:36:47.862Z"), "ticker" -> "WRGB"),
        Map("market" -> "AMEX", "roundedLastSale" -> 42.5, "lastSale" -> 42.5934, "lastSaleTime" -> dateOf("2022-09-04T23:36:47.865Z"), "ticker" -> "ESCN")
      ))
    }

    it("should extract rows from a table variable") {
      val (_, _, device_?) = LollypopVM.executeSQL(Scope(),
        """|declare table stocks(symbol: String(5), exchange: String(6), lastSale: Double) =
           | values ("AMD", "NASDAQ", 67.55), ("AAPL", "NYSE", 123.55), ("GE", "NASDAQ", 89.55),
           |        ("PEREZ", "OTCBB", 0.001), ("AMZN", "NYSE", 1234.55), ("INTC", "NYSE", 56.55)
           |from @@stocks
           |""".stripMargin)
      assert(Option(device_?).collect { case d: RowCollection => d }.toList.flatMap(_.toMapGraph) == List(
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 67.55),
        Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55),
        Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 89.55),
        Map("symbol" -> "PEREZ", "exchange" -> "OTCBB", "lastSale" -> 0.001),
        Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55)
      ))
    }

    it("should copy rows from a source to a target") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|declare table travelers(lastName String(12), firstName String(12), destAirportCode String(3))
            |containing values ('JONES', 'GARRY', 'SNA'), ('JONES', 'DEBBIE', 'SNA'),
            |       ('JONES', 'TAMERA', 'SNA'), ('JONES', 'ERIC', 'SNA'),
            |       ('ADAMS', 'KAREN', 'DTW'), ('ADAMS', 'MIKE', 'DTW'),
            |       ('JONES', 'SAMANTHA', 'BUR'), ('SHARMA', 'PANKAJ', 'LAX')
            |
            |declare table travelersJones(lastName String(12), firstName String(12), destAirportCode String(3))
            |this.toTable().show()
            |
            |from @@travelers
            |where lastName is 'JONES'
            |limit 4
            |into @@travelersJones
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("lastName" -> "JONES", "firstName" -> "GARRY", "destAirportCode" -> "SNA"),
        Map("lastName" -> "JONES", "firstName" -> "DEBBIE", "destAirportCode" -> "SNA"),
        Map("lastName" -> "JONES", "firstName" -> "TAMERA", "destAirportCode" -> "SNA"),
        Map("lastName" -> "JONES", "firstName" -> "ERIC", "destAirportCode" -> "SNA")
      ))
    }

    it("should apply a function to rows from a source") {
      val (_, _, results) = LollypopVM.searchSQL(ctx.createRootScope(),
        s"""|declare table travelers(lastName String(12), firstName String(12), destAirportCode String(3)) containing (
            ||----------------------------------------|
            || lastName | firstName | destAirportCode |
            ||----------------------------------------|
            || JONES    | GARRY     | SNA             |
            || JONES    | DEBBIE    | SNA             |
            || JONES    | TAMERA    | SNA             |
            || JONES    | ERIC      | SNA             |
            || ADAMS    | KAREN     | DTW             |
            || ADAMS    | MIKE      | DTW             |
            || JONES    | SAMANTHA  | BUR             |
            || SHARMA   | PANKAJ    | LAX             |
            ||----------------------------------------|
            |)
            |declare table manifest(line: String)
            |each row in (@@travelers order by destAirportCode) {
            |   insert into @@manifest (line)
            |   values ("[{{__id}}] Destination: {{destAirportCode}}, Name: {{lastName}}, {{firstName}}")
            |}
            |manifest
            |""".stripMargin)
      results.tabulate().foreach(logger.info)
      assert(results.toMapGraph == List(
        Map("line" -> "[0] Destination: BUR, Name: JONES, SAMANTHA"),
        Map("line" -> "[1] Destination: DTW, Name: ADAMS, KAREN"),
        Map("line" -> "[2] Destination: DTW, Name: ADAMS, MIKE"),
        Map("line" -> "[3] Destination: LAX, Name: SHARMA, PANKAJ"),
        Map("line" -> "[4] Destination: SNA, Name: JONES, ERIC"),
        Map("line" -> "[5] Destination: SNA, Name: JONES, GARRY"),
        Map("line" -> "[6] Destination: SNA, Name: JONES, DEBBIE"),
        Map("line" -> "[7] Destination: SNA, Name: JONES, TAMERA")
      ))
    }

  }

  def dateOf(s: String): Date = DateHelper.parse(s)

}
