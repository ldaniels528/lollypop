package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.Infix
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class EveryTest extends AnyFunSpec with VerificationTools {

  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Every].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|every '2 seconds' out.println('Hello World')
           |""".stripMargin)
      assert(results == Every("2 seconds", Infix("out".f, "println".fx("Hello World"))))
    }

    it("should support decompile") {
      val model = Every("2 seconds", Infix("out".f, "println".fx("Hello World")))
      assert(model.toSQL == """every "2 seconds" out.println("Hello World")""")
    }

    it("should asynchronously execute code") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|var n = 0
           |val timer = every '20 millis' {
           |  n += 1
           |}
           |import "java.lang.Thread"
           |Thread.sleep(Long(1000))
           |timer.cancel()
           |n
           |""".stripMargin)
      assert(Option(result).collect { case n: Number => n.intValue() } exists(_  >= 50))
    }

  }

}
