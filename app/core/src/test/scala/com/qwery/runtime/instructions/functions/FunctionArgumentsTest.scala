package com.qwery.runtime.instructions.functions

import com.qwery.language.models.FieldRef
import com.qwery.runtime.QweryCompiler
import org.scalatest.funspec.AnyFunSpec

class FunctionArgumentsTest extends AnyFunSpec {

  describe(FunctionArguments.getClass.getSimpleName.replaceAll("[$]", "")) {

    it("should interpret: x") {
      val model = QweryCompiler().compile(
        """|x
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains List(
        FieldRef("x")
      ))
    }

    it("should interpret: ()") {
      val model = QweryCompiler().compile(
        """|()
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains Nil)
    }

    it("should interpret: (y)") {
      val model = QweryCompiler().compile(
        """|(y)
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains List(
        FieldRef("y")
      ))
    }

    it("should interpret: (x, y, z)") {
      val model = QweryCompiler().compile(
        """|(x, y, z)
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains List(
        FieldRef("x"),
        FieldRef("y"),
        FieldRef("z")
      ))
    }

  }

}
