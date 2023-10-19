package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.@@
import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.Infix
import com.qwery.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.qwery.runtime.instructions.operators.Plus
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DoWhileTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[DoWhile].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|do {
           |   out.println('Hello World')
           |   @cnt += 1
           |} while @cnt < 10
           |""".stripMargin)
      val cnt = @@("cnt")
      assert(results == DoWhile(
        condition = cnt < 10,
        code = ScopedCodeBlock(
          Infix("out".f, "println".fx("Hello World")),
          Plus(cnt, b = 1.v).doAndSet
        )
      ))
    }

    it("should support decompile") {
      val model = compiler.compile(
        """|do {
           |   out.println('Hello World')
           |   @cnt += 1
           |} while @cnt < 10
           |""".stripMargin)
      assert(model == DoWhile(condition = @@("cnt") < 10.v, code = ScopedCodeBlock(
        Infix("out".f, "println".fx("Hello World".v)),
        Plus(@@("cnt"), b = 1.v).doAndSet
      )))
    }

    it(s"should support execution") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),
        """|def roman(value: Int) := ("I" * value)
           |  .replaceAll("IIIII", "V")
           |  .replaceAll("IIII", "IV")
           |  .replaceAll("VV", "X")
           |  .replaceAll("VIV", "IX")
           |  .replaceAll("XXXXX", "L")
           |  .replaceAll("XXXX", "XL")
           |  .replaceAll("LL", "C")
           |  .replaceAll("LXL", "XC")
           |  .replaceAll("CCCCC", "D")
           |  .replaceAll("CCCC", "CD")
           |  .replaceAll("DD", "M")
           |  .replaceAll("DCD", "CM")
           |
           |let cnt: Int = 1
           |do {
           |   out.println('{{ roman(Int(cnt)) }}. Hello World')
           |   cnt += 1
           |} while cnt < 5
           |select cnt
           |""".stripMargin)
      assert(results.toMapGraph == List(Map("cnt" -> 5)))
    }

  }

}
