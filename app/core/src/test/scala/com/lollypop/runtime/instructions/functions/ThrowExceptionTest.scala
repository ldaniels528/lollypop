package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.runtime.instructions.expressions.New
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ThrowExceptionTest extends AnyFunSpec {

  describe(classOf[ThrowException].getSimpleName) {

    it("should compile: throw new `java.lang.RuntimeException`('A processing error occurred')") {
      val model = LollypopCompiler().compile(
        """|throw new `java.lang.RuntimeException`('A processing error occurred')
           |""".stripMargin)
      assert(model == ThrowException(New("java.lang.RuntimeException", "A processing error occurred".v)))
    }

    it("should decompile: throw(new `java.lang.RuntimeException`('A processing error occurred'))") {
      val model = ThrowException(New("java.lang.RuntimeException", List("A processing error occurred".v)))
      assert(model.toSQL == """throw new `java.lang.RuntimeException`("A processing error occurred")""")
    }

    it("should execute: throw new `java.lang.RuntimeException`('A processing error occurred')") {
      assertThrows[RuntimeException] {
        LollypopVM.executeSQL(Scope(),
          """|throw new `java.lang.IllegalArgumentException`("A processing error occurred")
             |""".stripMargin)
      }
    }

  }

}
