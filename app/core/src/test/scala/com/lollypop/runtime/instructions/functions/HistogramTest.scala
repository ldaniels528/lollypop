package com.lollypop.runtime.instructions.functions

import com.lollypop.language.LifestyleExpressions
import com.lollypop.language.implicits._
import com.lollypop.runtime.LollypopCompiler
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import org.scalatest.funspec.AnyFunSpec

class HistogramTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Histogram].getSimpleName) {

    it("should produce a list of ranges") {
      val ranges = Histogram.computeRanges(Array(0, 1, 5, 10, 20, 100, 250, 1000))
      assert(ranges == List((0, 1), (1, 5), (5, 10), (10, 20), (20, 100), (100, 250), (250, 1000)))
    }

    it("should determine the index of a value within a list of ranges") {
      val ranges = Histogram.computeRanges(Array(0, 1, 5, 10, 20))
      val index = Histogram.computeIndex(ranges, 8)
      assert(index contains 2)
    }

    it("should compile a Histogram") {
      val model = compiler.compile(
        """|Histogram(lastSale, [0, 1, 5, 10, 20, 100, 250, 1000])
           |""".stripMargin)
      assert(model == Histogram("lastSale".f, ArrayLiteral(0, 1, 5, 10, 20, 100, 250, 1000)))
    }

  }
}
