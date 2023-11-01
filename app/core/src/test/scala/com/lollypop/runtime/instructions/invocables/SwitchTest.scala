package com.lollypop.runtime.instructions.invocables

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class SwitchTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Switch].getSimpleName) {

    it("should execute: switch(1)") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|value = 3
           |switch(value)
           |    case 0 then 'No'
           |    case 1 then 'Yes'
           |    case n then 'Maybe - {{n}}'
           |""".stripMargin)
      assert(rv == "Maybe - 3")
    }

    it("should execute: switch('Hello')") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|greeting = 'Hello'
           |switch greeting
           |    case 'Hola' then 'Spanish'
           |    case 'Hallo' then 'German'
           |    case s => s.startsWith('Hell') then 'English'
           |    case _ then 'Unrecognized'
           |""".stripMargin)
      assert(rv == "English")
    }

    it("should execute: switch(5.7)") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|value = 5.7
           |switch value
           |    case n => n < 5.0 ~> 'Yes - {{n}}'
           |    case n => n >= 5.0 and n <= 6.0 ~> 'Maybe - {{n}}'
           |    case n ~> 'No - {{n}}'
           |""".stripMargin)
      assert(rv == "Maybe - 5.7")
    }

    it("should execute: switch(new StockQ('ABC', 'AMEX', 78.23))") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|class StockQ(symbol: String, exchange: String, lastSale: Double)
           |switch new StockQ("ABC", "AMEX", 78.23)
           |    case p => p matches StockQ("ABC", "AMEX", _ => true) ~> p.lastSale
           |    case _ ~> 0.0
           |""".stripMargin)
      assert(rv == 78.23)
    }

    it("should execute: switch(new StockQ('YORKIE', 'NYSE', 999.99))") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|class StockQ(symbol: String, exchange: String, lastSale: Double)
           |switch new StockQ('YORKIE', 'NYSE', 999.99)
           |    case p => p matches StockQ(_ => true, "OTCBB", _ => true) ~> 'OT'
           |    case p => p matches StockQ(_ => true, "OTHER_OTC", _ => true) ~> 'OT'
           |    case p => p matches StockQ(_ => true, "AMEX", _ => true) ~> 'AM'
           |    case p => p matches StockQ(_ => true, "NASDAQ", _ => true) ~> 'ND'
           |    case p => p matches StockQ(_ => true, "NYSE", _ => true) ~> 'NY'
           |    case _ ~> 'NA'
           |""".stripMargin)
      assert(rv == "NY")
    }

  }

}
