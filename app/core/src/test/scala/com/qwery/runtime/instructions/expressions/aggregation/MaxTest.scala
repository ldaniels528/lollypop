package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.aggregation.Max
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class MaxTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Max].getSimpleName) {

    it("should support retrieve the count of active rows within a table") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
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
           |max(stocks#lastSale)
           |""".stripMargin)
      assert(result == 116.24)
    }

  }

}