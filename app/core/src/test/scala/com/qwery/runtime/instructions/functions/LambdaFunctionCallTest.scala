package com.qwery.runtime.instructions.functions

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.language.models.{@@, CodeBlock, Column, Literal}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.LambdaFunctionCall
import com.qwery.runtime.instructions.operators.Plus
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class LambdaFunctionCallTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[LambdaFunctionCall].getSimpleName) {

    it("should decompile to SQL: (name: String) => 'Hello ' + @name('World')") {
      val call = LambdaFunctionCall(
        AnonymousFunction(
          params = List(Column("name String")),
          code = Plus("Hello ".v, @@("name"))
        ),
        args = List("World".v)
      )
      assert(call.toSQL ===
        """|(name: String) => "Hello " + @name("World")
           |""".stripMargin.trim)
    }

    it("should decompile: ((name: String) => { select name })('Tom')") {
      val call = LambdaFunctionCall(
        AnonymousFunction(
          params = List(Column("name String")),
          code = CodeBlock(Select(fields = Seq("name".f)))
        ),
        args = List("Tom".v)
      )
      assert(call.toSQL ===
        """|(name: String) => {
           |  select name
           |}("Tom")""".stripMargin
      )
    }

    it("should execute an anonymous function via precompiled models") {
      implicit val scope: Scope = Scope()
      val call = LambdaFunctionCall(
        AnonymousFunction(
          params = List(Column("name String")),
          code = Plus(Literal("Hello "), @@("name"))
        ),
        args = List(Literal("World"))
      )
      val result = QweryVM.execute(scope, call)._3
      assert(result == "Hello World")
    }

    it("should compile and execute: (n: Int) => { n * n }(5)") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|set @result = (n: Int) => { n * n }(5)
           |select @result
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("result" -> 25.0)))
    }

    it("should compile and execute: (name: String) => { select name }('Tom')") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|((name: String) => { select name })('Tom')
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("name" -> "Tom")))
    }

    it("should compile and execute: ((first: String, last: String) => { select first, last })('Tom', 'Smith')") {
      val (_, _, result) = QweryVM.searchSQL(Scope(),
        """|((first: String, last: String) => { select first, last })('Tom', 'Smith')
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("first" -> "Tom", "last" -> "Smith")))
    }

  }

}
