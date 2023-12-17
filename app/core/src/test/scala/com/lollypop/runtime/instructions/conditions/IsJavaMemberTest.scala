package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.{TokenStream, _}
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class IsJavaMemberTest extends AnyFunSpec {

  describe(classOf[IsJavaMember].getSimpleName) {

    it("should compile: object.?intValue()") {
      val result = LollypopCompiler().nextExpression(TokenStream("object.?intValue()"))
      assert(result contains IsJavaMember("object".f, NamedFunctionCall(name = "intValue", args = Nil)))
    }

    it("should compile: object.?MAX_VALUE") {
      val result = LollypopCompiler().nextExpression(TokenStream("object.?MAX_VALUE"))
      assert(result contains IsJavaMember("object".f, "MAX_VALUE".f))
    }

    it("should decompile: object.?intValue()") {
      val model = IsJavaMember("object".f, NamedFunctionCall(name = "intValue", args = Nil))
      assert(model.toSQL == "object.?intValue()")
    }

    it("should decompile: object.?MAX_VALUE") {
      val model = IsJavaMember("object".f, "MAX_VALUE".f)
      assert(model.toSQL == "object.?MAX_VALUE")
    }

    it("should evaluate: object.?intValue()") {
     val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val object = 89.77
           |object.?intValue()
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate: object.?MAX_VALUE") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val object = 12345
           |object.?MAX_VALUE
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate: object.?MIN_VALUE as false") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|val object = "Hello World"
           |object.?MIN_VALUE
           |""".stripMargin)
      assert(result == false)
    }

  }

}
