package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NEGTest extends AnyFunSpec {

  describe(classOf[NEG].getSimpleName) {

    it("should compile: -(7 + 9)") {
      val model = LollypopCompiler().compile(
        """|-(7 + 9)
           |""".stripMargin)
      assert(model == NEG(7.v + 9.v))
    }

    it("should decompile: -(7 + 9)") {
      val model = NEG(7.v + 9.v)
      assert(model.toSQL == "-(7 + 9)")
    }

    it("should execute: -(7 + 9)") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|-(7 + 9)
           |""".stripMargin)
      assert(result == -16)
    }

  }

}
