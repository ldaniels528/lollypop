package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.aggregation.Count
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CountTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Count].getSimpleName) {

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
           |count(stocks)
           |""".stripMargin)
      assert(result == 5)
    }

  }

}
