package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.{@@, CodeBlock, Column, Literal}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.LambdaFunctionCall
import com.lollypop.runtime.instructions.operators.Plus
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class LambdaFunctionCallTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

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
      val result = LollypopVM.execute(scope, call)._3
      assert(result == "Hello World")
    }

    it("should compile and execute: (n: Int) => { n * n }(5)") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        """|set @result = (n: Int) => { n * n }(5)
           |select @result
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("result" -> 25.0)))
    }

    it("should compile and execute: (name: String) => { select name }('Tom')") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        """|((name: String) => { select name })('Tom')
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("name" -> "Tom")))
    }

    it("should compile and execute: ((first: String, last: String) => { select first, last })('Tom', 'Smith')") {
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        """|((first: String, last: String) => { select first, last })('Tom', 'Smith')
           |""".stripMargin
      )
      assert(result.toMapGraph == List(Map("first" -> "Tom", "last" -> "Smith")))
    }

  }

}
