package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.@@
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.jvm.ClassOf
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class InfixTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Infix].getSimpleName) {

    it("should decompile Java static fields") {
      verify("select classOf('java.awt.Color').BLACK")
    }

    it("should decompile Java instance fields") {
      verify("select @myValue.value")
    }

    it("should compile Java instance fields") {
      val results = compiler.compile("select @myValue.value")
      assert(results == Select(fields = Seq(Infix(instance = @@("myValue"), member = "value".f))))
    }

    it("should compile Java static fields") {
      val results = compiler.compile("select classOf('java.awt.Color').BLACK")
      assert(results == Select(fields = Seq(Infix(instance = ClassOf("java.awt.Color".v), member = "BLACK".f))))
    }

    it("should compile Java instance method calls") {
      val results = compiler.compile("select @offScreen.setColor(@color)")
      assert(results == Select(fields = Seq(
        Infix(instance = @@("offScreen"), member = NamedFunctionCall(name = "setColor", args = List(@@("color"))))
      )))
    }

    it("should compile Java static method calls") {
      val results = compiler.compile("select classOf('org.jsoup.Jsoup').parse(@file, 'UTF-8')")
      assert(results == Select(fields = Seq(
          Infix(instance = ClassOf("org.jsoup.Jsoup".v), member = NamedFunctionCall(name = "parse", args = List(@@("file"), "UTF-8".v)))
      )))
    }

    it("should decompile Java static method calls") {
      verify("select classOf('org.jsoup.Jsoup').parse(@file, 'UTF-8')")
    }

    it("should decompile Java instance method calls") {
      verify("select @offScreen.setColor(@color)")
    }

    it("should evaluate Java static method calls") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|classOf("java.lang.Runtime").getRuntime()
           |""".stripMargin
      )
      assert(result == Runtime.getRuntime)
    }

    it("should evaluate Java instance method calls") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|'Hello World'.substring(0, 5)
           |""".stripMargin
      )
      assert(result == "Hello")
    }

    it("should retrieve a value from a row") {
      val (_, _, symbol) = LollypopVM.executeSQL(Scope(),
        """|val stocks = (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | AAXX   | NYSE     |    56.12 |
           |  | UPEX   | NYSE     |   116.24 |
           |  | XYZ    | AMEX     |    31.95 |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |stocks(3).symbol
           |""".stripMargin)
      assert(symbol == "JUNK")
    }

  }

}
