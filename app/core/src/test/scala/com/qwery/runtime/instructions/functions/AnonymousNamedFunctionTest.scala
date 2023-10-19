package com.qwery.runtime.instructions.functions

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AnonymousNamedFunctionTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

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
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val f = isDefined
           |val y = 5
           |f(y)
           |""".stripMargin)
      assert(result == true)
    }

  }

}
