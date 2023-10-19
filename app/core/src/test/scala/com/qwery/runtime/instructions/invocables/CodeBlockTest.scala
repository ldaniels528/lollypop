package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.CodeBlock
import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.operators.Plus
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CodeBlockTest extends AnyFunSpec with VerificationTools {

  describe(classOf[CodeBlock].getSimpleName) {

    it("should not carry-over scope changes") {
      val codeBlock = CodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        SetAnyVariable(ref = "z".f, Plus("x".f, "y".f))
      )
      val (scope, _, _) = QweryVM.execute(Scope(), codeBlock)
      assert(scope.resolveAs[Int](path = "x").isEmpty)
      assert(scope.resolveAs[Int](path = "y").isEmpty)
      assert(scope.resolveAs[Double](path = "z").isEmpty)
    }

    it("should return the value of the last statement") {
      val codeBlock = CodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Plus("x".f, "y".f)
      )
      val (_, _, result) = QweryVM.execute(Scope(), codeBlock)
      assert(result == 12.0)
    }

    it("should capture the value of a return statement") {
      val codeBlock = CodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Return(Some(Plus("x".f, "y".f)))
      )
      val (_, _, result) = QweryVM.execute(Scope(), codeBlock)
      assert(result == 12.0)
    }

  }

}
