package com.lollypop.runtime.instructions.functions

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AnonymousNamedFunctionTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[AnonymousNamedFunction].getSimpleName) {

    it("should compile SQL into a model") {
      val model = compiler.compile("isDefined")
      assert(model == AnonymousNamedFunction("isDefined"))
    }

    it("should decompile a model into SQL") {
      val model = AnonymousNamedFunction("isDefined")
      assert(model.toSQL == "isDefined")
    }

    it("should execute an anonymous function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = isDefined
           |val y = 5
           |f(y)
           |""".stripMargin)
      assert(result == true)
    }

  }

}
