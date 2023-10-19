package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class BetweenTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()
  implicit val scope: Scope = Scope()

  describe(classOf[Between].getSimpleName) {

    it("""should evaluate 'Between(age, 25, 35)' as true""") {
      assert(Between("age".f, 25.v, 35.v).isTrue(scope.withVariable("age", Some(28), isReadOnly = true)))
    }

    it("""should evaluate 'Between(age, 25, 35)' as false""") {
      assert(Between("age".f, 25.v, 35.v).isFalse(scope.withVariable("age", 18.v, isReadOnly = true)))
    }

    it("""should negate: Between("greeting".f, "hello", "world")""") {
      assert(Between("greeting".f, "hello", "world").negate == Not(Between("greeting".f, "hello", "world")))
    }

    it("""should decompile 'Between(age, 25, 35)' to SQL""") {
      assert(Between("age".f, 25.v, 35.v).toSQL == "age between 25 and 35")
    }

    it("should decompile select .. where between") {
      verify(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear between '2000' and '2019'
           |""".stripMargin)
    }

    it("should support select .. where between") {
      import com.qwery.util.OptionHelper.implicits.risky._
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear between '2000' and '2019'
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
        from = DatabaseObjectRef("Customers"),
        where = Between("IPOYear".f, "2000", "2019")
      ))
    }

    it("should execute a query against a table literal using between") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
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
           |) where lastSale between 28.2808 and 42.5934
           |  order by lastSale desc
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("market" -> "AMEX", "roundedLastSale" -> 42.5, "lastSale" -> 42.5934, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.865Z"), "ticker" -> "ESCN"),
        Map("market" -> "AMEX", "roundedLastSale" -> 28.2, "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.864Z"), "ticker" -> "NFRK")
      ))
    }

  }

}
