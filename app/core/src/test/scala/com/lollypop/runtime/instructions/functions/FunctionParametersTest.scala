package com.lollypop.runtime.instructions.functions

import com.lollypop.language._
import com.lollypop.language.models.{ColumnType, Parameter}
import com.lollypop.runtime.LollypopCompiler
import org.scalatest.funspec.AnyFunSpec

class FunctionParametersTest extends AnyFunSpec {

  describe(classOf[FunctionParameters.type].getSimpleName) {

    it("should interpret: x") {
      val model = LollypopCompiler().compile(
        """|x
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "x", `type` = "Any".ct)
      ))
    }

    it("should interpret: ()") {
      val model = LollypopCompiler().compile(
        """|()
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains Nil)
    }

    it("should interpret: (y)") {
      val model = LollypopCompiler().compile(
        """|(y)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "y", `type` = "Any".ct)
      ))
    }

    it("should interpret: (x, y, z)") {
      val model = LollypopCompiler().compile(
        """|(x, y, z)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "x", `type` = "Any".ct),
        Parameter(name = "y", `type` = "Any".ct),
        Parameter(name = "z", `type` = "Any".ct)
      ))
    }

    it("should interpret: (n: Int)") {
      val model = LollypopCompiler().compile(
        """|(n: Int)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "n", `type` = "Int".ct)
      ))
    }

    it("should interpret: (a: Int, b: Float, c: Byte[])") {
      val model = LollypopCompiler().compile(
        """|(a: Int, b: Float, c: Byte[])
           |""".stripMargin)
      val params = FunctionParameters.unapply(model)
      assert(params contains List(
        Parameter(name = "a", `type` = "Int".ct),
        Parameter(name = "b", `type` = "Float".ct),
        Parameter(name = "c", `type` = ColumnType.array("Byte".ct))
      ))
    }

    it("should interpret: (s: String(80))") {
      val model = LollypopCompiler().compile(
        """|(s: String(80))
           |""".stripMargin)
      val params = FunctionParameters.unapply(model)
      assert(params contains List(
        Parameter(name = "s", `type` = "String".ct(80))
      ))
    }

    it("should reject: *") {
      val model = LollypopCompiler().compile(
        """|*
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (*)") {
      val model = LollypopCompiler().compile(
        """|(*)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (a, b, *)") {
      val model = LollypopCompiler().compile(
        """|(a, b, *)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (t, u, v : 3)") {
      val model = LollypopCompiler().compile(
        """|(t, u, v : 3)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: f()") {
      val model = LollypopCompiler().compile(
        """|f()
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

  }

}
