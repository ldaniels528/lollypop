package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.operators.Plus
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class InlineCodeBlockTest extends AnyFunSpec with VerificationTools {

  describe(classOf[InlineCodeBlock].getSimpleName) {

    it("should carry-over scope changes") {
      val codeBlock = InlineCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        SetAnyVariable(ref = "z".f, Plus("x".f, "y".f)))
      val (scope, _, _) = QweryVM.execute(Scope(), codeBlock)
      assert(scope.resolveAs[Int](path = "x") contains 7)
      assert(scope.resolveAs[Int](path = "y") contains 5)
      assert(scope.resolveAs[Double](path = "z") contains 12.0)
    }

    it("should return the value of the last statement") {
      val codeBlock = InlineCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Plus("x".f, "y".f))
      val (scope, _, result) = QweryVM.execute(Scope(), codeBlock)
      assert(result == 12.0)
    }

    it("should capture the value of a return statement") {
      val codeBlock = InlineCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Return(Some(Plus("x".f, "y".f))))
      assert(QweryVM.execute(Scope(), codeBlock)._3 == 12.0)
    }

  }

}
