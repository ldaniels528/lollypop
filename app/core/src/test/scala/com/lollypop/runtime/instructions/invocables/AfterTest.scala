package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.LollypopCompiler
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.Infix
import org.scalatest.funspec.AnyFunSpec

class AfterTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[After].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile(
        """|after '2 SECONDS' stdout.println('Hello World')
           |""".stripMargin)
      assert(results == After("2 SECONDS", Infix("stdout".f, "println".fx("Hello World"))))
    }

    it("should support decompile") {
      verify(
        """|after '2 SECONDS' stdout <=== 'Hello World'
           |""".stripMargin)
    }

  }

}

