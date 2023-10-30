package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.{Column, Parameter}
import com.lollypop.runtime.instructions.operators.Plus
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AnonymousFunctionTest extends AnyFunSpec {

  describe(classOf[AnonymousFunction].getSimpleName) {

    it("should compile SQL into a model") {
      val actual = LollypopCompiler().compile("(n: Int) => n + 1")
      val expected = AnonymousFunction(
        params = Seq(Parameter(name = "n", `type` = "Int".ct)),
        code = Plus("n".f, 1.v),
        origin = None)
      assert(actual == expected)
    }

    it("should decompile a model into SQL") {
      val model = AnonymousFunction(
        params = Seq(Parameter(name = "n", `type` = "Int".ct)),
        code = Plus("n".f, 1.v),
        origin = None)
      assert(model.toSQL == "(n: Int) => n + 1")
    }

    it("should execute an anonymous function inline") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|((n: Int) => n + 1)(5)
           |""".stripMargin)
      assert(result == 6)
    }

    it("should execute an anonymous function by reference") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = (n: Int) => n + 1
           |f(5)
           |""".stripMargin)
      assert(result == 6)
    }

    it("should convert an anonymous function into a Scala function") {
      val model = AnonymousFunction(
        params = Seq(Column(name = "n", `type` = "Int".ct)),
        code = Plus("n".f, 1.v),
        origin = None)
      assert(model.toScala.isInstanceOf[scala.Function1[_, _]])
    }

    it("should execute an anonymous (no parameters) as a Scala function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = () => "It works!"
           |val fx = f.toScala()
           |fx.apply()
           |""".stripMargin)
      assert(result == "It works!")
    }

    it("should execute an anonymous (1 parameter) as a Scala function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = s => s.reverse()
           |val fx = f.toScala()
           |fx.apply('Hello World')
           |""".stripMargin)
      assert(result == "dlroW olleH")
    }

    it("should execute an anonymous (3 parameters) as a Scala function") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = (x: Double, y: Double, z: Double) => (x * y) + z
           |val fx = f.toScala()
           |fx.apply(5, 7, 9)
           |""".stripMargin)
      assert(result == 44.0)
    }

    it("should pass an anonymous function into a Scala method") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val f = (s1: String, s2: String) => s1 + " " + s2
           |val scalaObject = objectOf("com.lollypop.runtime.instructions.functions.AnonymousFunctionTest")
           |scalaObject('Hello', 'World', f)
           |""".stripMargin)
      assert(result == "Hello World")
    }

  }

}

object AnonymousFunctionTest {

  def apply(s1: String, s2: String, fx: (String, String) => String): String = fx(s1, s2)

}
