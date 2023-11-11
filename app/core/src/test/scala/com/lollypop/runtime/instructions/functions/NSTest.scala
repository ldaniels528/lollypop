package com.lollypop.runtime.instructions.functions

import com.lollypop.language.LollypopUniverse
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import lollypop.io.Nodes
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class NSTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ctx = LollypopUniverse(isServerMode = true)
  private val rootScope = ctx.createRootScope()
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  private val node = Nodes().start()
  private val port = node.port

  describe(classOf[NS].getSimpleName) {

    it("should compile: ns('com.shocktrade.efx')") {
      val results = compiler.compile("ns('com.shocktrade.efx')")
      assert(results == NS("com.shocktrade.efx".v))
    }

    it("should decompile: ns('com.shocktrade.efx')") {
      verify("ns('com.shocktrade.efx')")
    }

    it("should load persistent objects into variables") {
      val stocksTable = DatabaseObjectRef("temp.system.RandomQuotes")
      val insertCount = 250
      val scope0 = Scope().withVariable(name = "insertCount", value = Some(insertCount))
      val (_, _, result1) = LollypopVM.executeSQL(scope0,
        s"""|drop if exists $stocksTable
            |create table $stocksTable (
            |   symbol: String(5),
            |   exchange: String(6),
            |   lastSale: Double,
            |   lastSaleTime: DateTime
            |)
            |
            |let cnt: Int = 0
            |[1 to insertCount].foreach((n: Int) => {
            |   insert into $stocksTable (lastSaleTime, lastSale, exchange, symbol)
            |   select lastSaleTime: new `java.util.Date`(),
            |          lastSale: scaleTo(100 * Random.nextDouble(0.99), 4),
            |          exchange: ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB'][Random.nextInt(4)],
            |          symbol: Random.nextString(['A' to 'Z'], 4)
            |})
            |
            |set stockQuotes = ns('$stocksTable')
            |select total: count(*) from @stockQuotes
            |""".stripMargin)
      val device_? = Option(result1).collect { case device: RowCollection => device }
      device_?.foreach(_.tabulate().foreach(logger.info))
    }

    it("should access remote collections") {
      val ref = DatabaseObjectRef(getTestTableName)
      val (_, _, device) = LollypopVM.searchSQL(rootScope,
        s"""|drop if exists $ref
            |create table $ref (
            |   symbol: String(7),
            |   exchange: String(5),
            |   lastSale: Double,
            |   code String(2)
            |)
            |
            |insert into $ref (symbol, exchange, lastSale, code)
            |values
            |  ('AAXX', 'NYSE', 56.12, 'A'), ('UPEX', 'NYSE', 116.24, 'A'), ('XYZ', 'AMEX',   31.9500, 'A'),
            |  ('JUNK', 'AMEX', 97.61, 'B'), ('RTX.OB',  'OTCBB', 1.93011, 'B'), ('ABC', 'NYSE', 1235.7650, 'B'),
            |  ('UNIB.OB', 'OTCBB',  9.11, 'C'), ('BRT.OB', 'OTCBB', 0.00123, 'C'), ('PLUMB', 'NYSE', 1009.0770, 'C')
            |
            |from ns("//0.0.0.0:$port/$ref")
            |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "code" -> "A"),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24, "code" -> "A"),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95, "code" -> "A"),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61, "code" -> "B"),
        Map("symbol" -> "RTX.OB", "exchange" -> "OTCBB", "lastSale" -> 1.93011, "code" -> "B"),
        Map("symbol" -> "ABC", "exchange" -> "NYSE", "lastSale" -> 1235.765, "code" -> "B"),
        Map("symbol" -> "UNIB.OB", "exchange" -> "OTCBB", "lastSale" -> 9.11, "code" -> "C"),
        Map("symbol" -> "BRT.OB", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "code" -> "C"),
        Map("symbol" -> "PLUMB", "exchange" -> "NYSE", "lastSale" -> 1009.077, "code" -> "C")
      ))
    }

  }

}
