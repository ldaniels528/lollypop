package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.QweryCompiler
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.Infix
import org.scalatest.funspec.AnyFunSpec

class AfterTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[After].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|after '2 SECONDS' out.println('Hello World')
           |""".stripMargin)
      assert(results == After("2 SECONDS", Infix("out".f, "println".fx("Hello World"))))
    }

    it("should support decompile") {
      verify(
        """|after '2 SECONDS' out.println 'Hello World'
           |""".stripMargin)
    }

  }

}

