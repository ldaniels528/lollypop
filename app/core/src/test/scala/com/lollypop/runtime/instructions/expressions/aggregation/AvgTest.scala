package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AvgTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Avg].getSimpleName) {

    it("should support retrieve the count of active rows within a table") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val stocks =
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |
           |avg(stocks#lastSale)
           |""".stripMargin)
      assert(result == 61.56139999999999)
    }

  }

}