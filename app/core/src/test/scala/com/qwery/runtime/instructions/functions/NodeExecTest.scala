package com.qwery.runtime.instructions.functions

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class NodeExecTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[NodeExec].getSimpleName) {

    it("should execute commands on a remote peer") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|val port = nodeStart()
           |after Interval('4 seconds') nodeStop(port)
           |nodeExec(port, "
           |from (
           |    |-------------------------------------------------------|
           |    | ticker | market | lastSale | lastSaleTime             |
           |    |-------------------------------------------------------|
           |    | NKWI   | OTCBB  |  98.9501 | 2022-09-04T23:36:47.846Z |
           |    | AQKU   | NASDAQ |  68.2945 | 2022-09-04T23:36:47.860Z |
           |    | WRGB   | AMEX   |  46.8355 | 2022-09-04T23:36:47.862Z |
           |    | ESCN   | AMEX   |  42.5934 | 2022-09-04T23:36:47.865Z |
           |    | NFRK   | AMEX   |  28.2808 | 2022-09-04T23:36:47.864Z |
           |    |-------------------------------------------------------|
           |) where lastSale < 30
           |")
           |""".stripMargin)
      result.tabulate().foreach(logger.info)
      assert(result.toMapGraph == List(
        Map("market" -> "AMEX", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.864Z"), "ticker" -> "NFRK")
      ))
    }

    it("should execute commands on multiple remote peers") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        s"""|# create a public macro
            |drop if exists stocksMacro
            |create macro stocksMacro := 'STOCKS %e:total' {
            |    declare table myQuotes(symbol: String(4), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
            |    var n: Int = 0
            |    while n < total {
            |        n += 1
            |        insert into @@myQuotes (lastSaleTime, lastSale, exchange, symbol)
            |        select lastSaleTime: DateTime(),
            |        lastSale: scaleTo(500 * Random.nextDouble(0.99), 4),
            |        exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
            |        symbol: Random.nextString(['A' to 'Z'], 4)
            |    }
            |    @@myQuotes
            |}
            |
            |// start the remote peers
            |val portA = nodeStart()
            |val portB = nodeStart()
            |
            |// setup a shutdown hook
            |after Interval('10 seconds') {
            |  nodeStop(portA)
            |  nodeStop(portB)
            |}
            |val stocksA = nodeExec(portA, "STOCKS 3")
            |val stocksB = nodeExec(portB, "STOCKS 3")
            |@@stocksA union @@stocksB
            |""".stripMargin)
      result.tabulate().foreach(logger.info)
    }

  }

}
