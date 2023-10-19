package com.qwery.runtime.instructions.operators

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Operation.RichOperation
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NEGTest extends AnyFunSpec {

  describe(classOf[NEG].getSimpleName) {

    it("should compile: -(7 + 9)") {
      val model = QweryCompiler().compile(
        """|-(7 + 9)
           |""".stripMargin)
      assert(model == NEG(7.v + 9.v))
    }

    it("should decompile: -(7 + 9)") {
      val model = NEG(7.v + 9.v)
      assert(model.toSQL == "-(7 + 9)")
    }

    it("should execute: -(7 + 9)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|-(7 + 9)
           |""".stripMargin)
      assert(result == -16)
    }

  }

}
