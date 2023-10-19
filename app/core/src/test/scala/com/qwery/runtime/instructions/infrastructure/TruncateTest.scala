package com.qwery.runtime.instructions.infrastructure

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class TruncateTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Truncate].getSimpleName) {

    it("should support compilation") {
      val results = compiler.compile("truncate stocks")
      assert(results == Truncate(ref = DatabaseObjectRef("stocks")))
    }

    it("should support de-compilation") {
      verify("truncate stocks")
    }

    it("should support execution") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|declare table results(symbol: String(5), exchange: String(6), lastSale: Double)
           |insert into @@results (symbol, exchange, lastSale)
           |values ('GMTQ', 'OTCBB', 0.1111), ('ABC', 'NYSE', 38.47), ('GE', 'NASDAQ', 57.89)
           |truncate @@results
           |select n: count(*) from @@results
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("n" -> 0)))
    }

  }

}
