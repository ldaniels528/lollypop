package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.Infix
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class EveryTest extends AnyFunSpec with VerificationTools {

  implicit val compiler: QweryCompiler = QweryCompiler()

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
      val (_, _, result) = QweryVM.executeSQL(Scope(),
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
