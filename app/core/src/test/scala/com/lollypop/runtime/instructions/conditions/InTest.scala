package com.lollypop.runtime.instructions.conditions

import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class InTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[IN].getSimpleName) {

    it("should decompile select .. where in [...]") {
      verify(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear in ['2000', '2001', '2003', '2008', '2019']
           |""".stripMargin)
    }

    it("should decompile select .. where in (select ..)") {
      verify(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers as C
           |where IPOYear in (select Year from EligibleYears)
           |""".stripMargin)
    }

    it("should support select .. where in (..)") {
      import com.lollypop.runtime.implicits.risky._
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers
           |where IPOYear in ['2000', '2001', '2003', '2008', '2019']
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
        from = DatabaseObjectRef("Customers"),
        where = IN("IPOYear".f, ArrayLiteral.from("2000", "2001", "2003", "2008", "2019"))
      ))
    }

    it("should support select .. where in (select ..)") {
      import com.lollypop.runtime.implicits.risky._
      val results = compiler.compile(
        """|select Symbol, Name, Sector, Industry, SummaryQuote
           |from Customers as C
           |where IPOYear in (select `Year` from EligibleYears)
           |""".stripMargin)
      assert(results == Select(
        fields = Seq("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
        from = DatabaseObjectRef("Customers"),
        where = IN("IPOYear".f, Select(fields = Seq("Year".f), from = DatabaseObjectRef("EligibleYears")))
      ))
    }

    it("should execute queries against table literals") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|set stocks = (
           ||-------------------------------------------------------------------------|
           || ticker | market | lastSale | roundedLastSale | lastSaleTime             |
           ||-------------------------------------------------------------------------|
           || AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
           || NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
           || ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------------------------|
           |)
           |@stocks where market in ["OTCBB"]
           |""".stripMargin)
      assert(device.toMapGraph == List(Map(
        "market" -> "OTCBB", "roundedLastSale" -> 98.9, "lastSale" -> 98.9501,
        "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.846Z"), "ticker" -> "NKWI"
      )))
    }

  }

}
