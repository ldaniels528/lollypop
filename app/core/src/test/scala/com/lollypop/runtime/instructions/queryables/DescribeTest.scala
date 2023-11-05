package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.functions.NS
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class DescribeTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Describe].getSimpleName) {

    it("should support compiling describe query") {
      val results = compiler.compile("describe (select 123, 'ABC')")
      assert(results == Describe(queryable = Select(fields = Seq(123, "ABC"))))
    }

    it("should support compiling describe reference") {
      val results = compiler.compile("describe ns('ldaniels.securities.stocks')")
      assert(results == Describe(NS("ldaniels.securities.stocks")))
    }

    it("should support decompiling describe query") {
      verify("describe (select 123, 'ABC')")
    }

    it("should support decompiling describe reference") {
      verify("describe ns('ldaniels.securities.stocks')")
    }

    it("should support decompiling describe variable reference") {
      verify("describe @stocks")
    }

    it("should perform describe a select without a source") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|describe (
           |  select symbol: "GMTQ",
           |         exchange: "OTCBB",
           |         lastSale: Random.nextDouble(0.99),
           |         lastSaleTime: DateTime(1631508164812),
           |         elapsedTime: Interval('9 seconds'),
           |         verified: true,
           |         testsAreFailing: false,
           |         alphabet: ['A' to 'Z']
           |)
           |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(
        Map("name" -> "symbol", "type" -> "String(4)"),
        Map("name" -> "exchange", "type" -> "String(5)"),
        Map("name" -> "lastSale", "type" -> "Double"),
        Map("name" -> "lastSaleTime", "type" -> "DateTime"),
        Map("name" -> "elapsedTime", "type" -> "Interval"),
        Map("name" -> "verified", "type" -> "Boolean"),
        Map("name" -> "testsAreFailing", "type" -> "Boolean"),
        Map("name" -> "alphabet", "type" -> "VarChar(26)")
      ))
    }

  }

}
