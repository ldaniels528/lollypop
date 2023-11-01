package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Parameter
import com.lollypop.language.{SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.expressions.{Infix, NamedFunctionCall, TransferFrom}
import com.lollypop.runtime.instructions.functions.AnonymousFunction
import com.lollypop.runtime.instructions.invocables.TryCatch.templateCard
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TryCatchTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[TryCatch].getSimpleName) {

    it("should parse: try connect() catch e => println(e.getMessage()) finally cleanup()") {
      val params = SQLTemplateParams(TokenStream("try connect() catch e => out <=== e.getMessage() finally cleanup()"), templateCard)
      assert(params.all == Map(
        "code" -> NamedFunctionCall(name = "connect", args = Nil),
        "onError" -> AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil)))
        ),
        "keywords" -> Set("try", "catch", "finally"),
        "finally" -> NamedFunctionCall(name = "cleanup", args = Nil)
      ))
    }

    it("should compile: try connect() catch e => println(e.getMessage())") {
      val model = compiler.compile(
        """|try connect() catch e => out <=== e.getMessage()
           |""".stripMargin)
      assert(model == TryCatch(
        code = NamedFunctionCall(name = "connect", args = Nil),
        onError = AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil))))
      ))
    }

    it("should compile: try connect() catch e => println(e.getMessage()) finally cleanup()") {
      import com.lollypop.util.OptionHelper.implicits.risky.value2Option
      val model = compiler.compile(
        """|try connect() catch e => out <=== e.getMessage() finally cleanup()
           |""".stripMargin)
      assert(model == TryCatch(
        code = NamedFunctionCall(name = "connect", args = Nil),
        onError = AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil)))
        ), `finally` = NamedFunctionCall(name = "cleanup", args = Nil)
      ))
    }

    it("should decompile: try connect() catch e => out <=== e.getMessage()") {
      val model = TryCatch(
        code = NamedFunctionCall(name = "connect", args = Nil),
        onError = AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil)))
        ))
      assert(model.toSQL == "try connect() catch (e: Any) => out <=== e.getMessage()")
    }

    it("should decompile: try connect() catch e => println(e.getMessage()) finally cleanup()") {
      import com.lollypop.util.OptionHelper.implicits.risky.value2Option
      val model = TryCatch(
        code = NamedFunctionCall(name = "connect", args = Nil),
        onError = AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil)))
        ), `finally` = NamedFunctionCall(name = "cleanup", args = Nil))
      assert(model.toSQL == "try connect() catch (e: Any) => out <=== e.getMessage() finally cleanup()")
    }

    it("should execute: try n += 1 catch e => println(e.getMessage())") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|var n = 1
           |try n += 1 catch e => out <=== e.getMessage()
           |n
           |""".stripMargin)
      assert(result == 2)
    }

    it("should execute: try n /= 0 catch e => stdout <=== e.getMessage()") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|var n = 1
           |try n /= 0 catch e => stdout <=== e.getMessage()
           |n
           |""".stripMargin)
      assert(result == 1)
    }

    it("should execute: try n /= 0 catch e => stdout <=== e.getMessage() finally n = -1") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|var n = 1
           |try n /= 0 catch e => stdout <=== e.getMessage() finally n = -1
           |n
           |""".stripMargin)
      assert(result == -1)
    }

    it("should execute try-catch-finally with case-when") {
      val (scope, _, result) = LollypopVM.executeSQL(Scope(),
        """|var n = 1
           |var error: String = null
           |try n / 0 catch e =>
           |  switch typeOf(e)
           |    case "java.lang.RuntimeException" ~>
           |      error = "RuntimeException: {{e.getMessage()}}"
           |    case _ ~> error = e.getMessage()
           |finally n = -1
           |n
           |""".stripMargin)
      assert(scope.resolveAs[String]("error").exists(_.startsWith("Division by zero")) && result == -1)
    }

  }

}