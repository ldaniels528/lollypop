package com.lollypop.runtime.instructions.conditions

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class ExistsTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  implicit val scope: Scope = Scope()

  describe(classOf[Exists].getSimpleName) {

    it("""should negate: exists(Select(fields = Seq("a".f), where = "a".f >= 5))""") {
      assert(Exists(Select(fields = Seq("a".f), where = "a".f >= 5)).negate == Not(Exists(Select(fields = Seq("a".f), where = "a".f >= 5))))
    }

    it("should decompile select .. exists(..)") {
      verify(
        """|select * from Departments
           |where exists(select employee_id from Employees where role == 'MANAGER')
           |""".stripMargin)
    }

    it("should support select .. exists(..)") {
      val results = compiler.compile(
        """|select * from Departments
           |where exists(select employee_id from Employees where role == 'MANAGER')
           |""".stripMargin)
      assert(results ==
        Select(Seq("*".f),
          from = DatabaseObjectRef("Departments"),
          where = Exists(
            Select(fields = Seq("employee_id".f), from = DatabaseObjectRef("Employees"), where = "role".f === "MANAGER")
          )
        ))
    }

    it("should execute queries against table literals") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|set stocks = (
           ||-------------------------------------------------------------------------|
           || ticker | market | lastSale | roundedLastSale | lastSaleTime             |
           ||-------------------------------------------------------------------------|
           || NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
           || AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
           || WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
           || ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
           || NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
           ||-------------------------------------------------------------------------|
           |)
           |@stocks where lastSale > 90 and exists(select ticker from @stocks where market is 'OTCBB')
           |""".stripMargin)
      assert(device.toMapGraph == List(Map(
        "market" -> "OTCBB", "roundedLastSale" -> 98.9, "lastSale" -> 98.9501,
        "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.846Z"), "ticker" -> "NKWI"
      )))
    }

  }

}
