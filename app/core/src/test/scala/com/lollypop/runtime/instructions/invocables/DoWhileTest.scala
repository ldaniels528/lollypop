package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.Infix
import com.lollypop.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.lollypop.runtime.instructions.operators.Plus
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DoWhileTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[DoWhile].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|do {
           |   out.println('Hello World')
           |   cnt += 1
           |} while cnt < 10
           |""".stripMargin)
      val cnt = "cnt".f
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
           |   cnt += 1
           |} while cnt < 10
           |""".stripMargin)
      assert(model == DoWhile(condition = "cnt".f < 10.v, code = ScopedCodeBlock(
        Infix("out".f, "println".fx("Hello World".v)),
        Plus("cnt".f, b = 1.v).doAndSet
      )))
    }

    it(s"should support execution") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
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
           |   stdout <=== '{{ roman(Int(cnt)) }}. Hello World'
           |   cnt += 1
           |} while cnt < 5
           |select cnt
           |""".stripMargin)
      assert(results.toMapGraph == List(Map("cnt" -> 5)))
    }

  }

}
