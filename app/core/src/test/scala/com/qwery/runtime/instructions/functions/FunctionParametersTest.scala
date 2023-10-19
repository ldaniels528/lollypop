package com.qwery.runtime.instructions.functions

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.language.models.{ColumnType, Parameter}
import com.qwery.runtime.QweryCompiler
import org.scalatest.funspec.AnyFunSpec

class FunctionParametersTest extends AnyFunSpec {

  describe(classOf[FunctionParameters.type].getSimpleName) {

    it("should interpret: x") {
      val model = QweryCompiler().compile(
        """|x
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "x", `type` = "Any".ct)
      ))
    }

    it("should interpret: ()") {
      val model = QweryCompiler().compile(
        """|()
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains Nil)
    }

    it("should interpret: (y)") {
      val model = QweryCompiler().compile(
        """|(y)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "y", `type` = "Any".ct)
      ))
    }

    it("should interpret: (x, y, z)") {
      val model = QweryCompiler().compile(
        """|(x, y, z)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "x", `type` = "Any".ct),
        Parameter(name = "y", `type` = "Any".ct),
        Parameter(name = "z", `type` = "Any".ct)
      ))
    }

    it("should interpret: (n: Int)") {
      val model = QweryCompiler().compile(
        """|(n: Int)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model) contains List(
        Parameter(name = "n", `type` = "Int".ct)
      ))
    }

    it("should interpret: (a: Int, b: Float, c: Byte[])") {
      val model = QweryCompiler().compile(
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
      val model = QweryCompiler().compile(
        """|(s: String(80))
           |""".stripMargin)
      val params = FunctionParameters.unapply(model)
      assert(params contains List(
        Parameter(name = "s", `type` = "String".ct(80))
      ))
    }

    it("should reject: *") {
      val model = QweryCompiler().compile(
        """|*
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (*)") {
      val model = QweryCompiler().compile(
        """|(*)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (a, b, *)") {
      val model = QweryCompiler().compile(
        """|(a, b, *)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: (t, u, v : 3)") {
      val model = QweryCompiler().compile(
        """|(t, u, v : 3)
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

    it("should reject: f()") {
      val model = QweryCompiler().compile(
        """|f()
           |""".stripMargin)
      assert(FunctionParameters.unapply(model).isEmpty)
    }

  }

}
