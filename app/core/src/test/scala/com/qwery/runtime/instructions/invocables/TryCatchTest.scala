package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Parameter
import com.qwery.language.{SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.expressions.{Infix, NamedFunctionCall, TransferFrom}
import com.qwery.runtime.instructions.functions.AnonymousFunction
import com.qwery.runtime.instructions.invocables.TryCatch.templateCard
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TryCatchTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

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
      import com.qwery.util.OptionHelper.implicits.risky.value2Option
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
      import com.qwery.util.OptionHelper.implicits.risky.value2Option
      val model = TryCatch(
        code = NamedFunctionCall(name = "connect", args = Nil),
        onError = AnonymousFunction(
          params = Seq(Parameter(name = "e", `type` = "Any".ct)),
          code = TransferFrom("out".f, Infix("e".f, NamedFunctionCall(name = "getMessage", args = Nil)))
        ), `finally` = NamedFunctionCall(name = "cleanup", args = Nil))
      assert(model.toSQL == "try connect() catch (e: Any) => out <=== e.getMessage() finally cleanup()")
    }

    it("should execute: try n += 1 catch e => println(e.getMessage())") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|var n = 1
           |try n += 1 catch e => out <=== e.getMessage()
           |n
           |""".stripMargin)
      assert(result == 2)
    }

    it("should execute: try n /= 0 catch e => println(e.getMessage())") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|var n = 1
           |try n /= 0 catch e => out <=== e.getMessage()
           |n
           |""".stripMargin)
      assert(result == 1)
    }

    it("should execute: try n /= 0 catch e => println(e.getMessage()) finally n = -1") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|var n = 1
           |try n /= 0 catch e => out <=== e.getMessage() finally n = -1
           |n
           |""".stripMargin)
      assert(result == -1)
    }

    it("should execute try-catch-finally with case-when") {
      val (scope, _, result) = QweryVM.executeSQL(Scope(),
        """|var n = 1
           |var error: String = null
           |try n / 0 catch e => {
           |  case
           |    when typeOf(e) is "java.lang.RuntimeException" -> error = "RuntimeException: {{e.getMessage()}}"
           |    else error = e.getMessage()
           |  end
           |}
           |finally n = -1
           |n
           |""".stripMargin)
      assert(scope.resolveAs[String]("error").exists(_.startsWith("Division by zero")) && result == -1)
    }

  }

}