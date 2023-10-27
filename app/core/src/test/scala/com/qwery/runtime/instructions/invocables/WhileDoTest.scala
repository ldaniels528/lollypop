package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.CodeBlock
import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.Infix
import com.qwery.runtime.instructions.operators.ComputeAndSet.ComputeAndSetSugar
import com.qwery.runtime.instructions.operators.Plus
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class WhileDoTest extends AnyFunSpec with VerificationTools {

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[WhileDo].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|while cnt < 10 do
           |begin
           |   out.println('Hello World')
           |   cnt += 1
           |end
           |""".stripMargin)
      val cnt = "cnt".f
      assert(results == WhileDo(
        condition = cnt < 10,
        CodeBlock(
          Infix("out".f, "println".fx("Hello World")),
          Plus(cnt, b = 1.v).doAndSet
        )
      ))
    }

    it("should support decompile to model") {
      val model = compiler.compile(
        """|while cnt < 10 do {
           |   out.println('Hello World')
           |   cnt += 1
           |}
           |""".stripMargin)
      val cnt = "cnt".f
      assert(model == WhileDo(cnt < 10.v, CodeBlock(
        Infix("out".f, "println".fx("Hello World".v)),
        Plus(cnt, b = 1.v).doAndSet
      )))
    }

    it("should support decompile to SQL") {
      val model = compiler.compile(
        """|while cnt < 10 {
           |   println('Hello World')
           |   cnt += 1
           |}
           |""".stripMargin).toSQL
      assert(model ==
        """|while cnt < 10 do {
           |  println("Hello World")
           |  cnt += 1
           |}
           |""".stripMargin.trim)
    }

    it(s"should support execution") {
      val (_, _, results) = QweryVM.executeSQL(Scope(),
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
           |while cnt < 5 do {
           |   stdout.println('{{ roman(cnt) }}. Hallo Monde')
           |   cnt += 1
           |}
           |cnt
           |""".stripMargin)
      assert(results == 5)
    }

  }

}
