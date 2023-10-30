package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.FieldRef
import com.lollypop.runtime.LollypopCompiler
import org.scalatest.funspec.AnyFunSpec

class FunctionArgumentsTest extends AnyFunSpec {

  describe(FunctionArguments.getClass.getSimpleName.replaceAll("[$]", "")) {

    it("should interpret: x") {
      val model = LollypopCompiler().compile(
        """|x
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains List(
        FieldRef("x")
      ))
    }

    it("should interpret: ()") {
      val model = LollypopCompiler().compile(
        """|()
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains Nil)
    }

    it("should interpret: (y)") {
      val model = LollypopCompiler().compile(
        """|(y)
           |""".stripMargin)
      assert(FunctionArguments.unapply(model) contains List(
        FieldRef("y")
      ))
    }

    it("should interpret: (x, y, z)") {
      val model = LollypopCompiler().compile(
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
