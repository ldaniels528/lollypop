package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class BetwixtTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  implicit val scope: Scope = Scope()

  describe(classOf[Betwixt].getSimpleName) {

    it("""should evaluate 'Betwixt(age, 25, 35)' as true""") {
      assert(Betwixt("age".f, 25.v, 35.v).isTrue(scope.withVariable("age", 25.v, isReadOnly = true)))
    }

    it("""should evaluate 'Betwixt(age, 25, 35)' as false""") {
      assert(Betwixt("age".f, 25.v, 35.v).isFalse(scope.withVariable("age", Some(35), isReadOnly = true)))
    }

    it("""should negate: Betwixt("greeting".f, "hello", "world")""") {
      assert(Betwixt("greeting".f, "hello", "world").negate == Not(Betwixt("greeting".f, "hello", "world")))
    }

    it("""should decompile 'Betwixt(age, 25, 35)' to SQL""") {
      assert(Betwixt("age".f, 25.v, 35.v).toSQL == "age betwixt 25 and 35")
    }

    it("should decompile select .. where betwixt") {
      verify(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear betwixt '2000' and '2019'
           |""".stripMargin)
    }

    it("should support select .. where betwixt") {
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear betwixt '2000' and '2019'
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
        from = DatabaseObjectRef("Customers"),
        where = Betwixt("IPOYear".f, "2000", "2019")
      ))
    }

    it("should execute a query against a table literal using betwixt") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|from (
           ||-------------------------------------------------------------------------|
           || ticker | market | lastSale | roundedLastSale | lastSaleTime             |
           ||-------------------------------------------------------------------------|
           || NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
           || AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
           || ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------------------------|
           |) where lastSale betwixt 28.2808 and 42.5934
           |""".stripMargin)
      assert(device.toMapGraph == List(Map(
        "market" -> "AMEX", "roundedLastSale" -> 28.2, "lastSale" -> 28.2808,
        "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.864Z"), "ticker" -> "NFRK"
      )))
    }

  }

}
