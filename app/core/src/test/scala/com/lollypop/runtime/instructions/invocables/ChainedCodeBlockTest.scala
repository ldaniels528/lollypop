package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.operators.Plus
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ChainedCodeBlockTest extends AnyFunSpec with VerificationTools {

  describe(classOf[ChainedCodeBlock].getSimpleName) {

    it("should decompile an instruction model into SQL") {
      val model = ChainedCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        SetAnyVariable(ref = "z".f, Plus("x".f, "y".f)))
      assert(model.toSQL == "set x = 7 && set y = 5 && set z = x + y")
    }

    it("should compile SQL into an instruction model") {
      val model = LollypopCompiler().compile("set x = 7 && set y = 5 && set z = x + y")
      assert(model == InlineCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        SetAnyVariable(ref = "z".f, Plus("x".f, "y".f))
      ))
    }

    it("should carry-over scope changes") {
      val codeBlock = ChainedCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        SetAnyVariable(ref = "z".f, Plus("x".f, "y".f)))
      val (scope, _, _) = LollypopVM.execute(Scope(), codeBlock)
      assert(scope.resolveAs[Int](path = "x") contains 7)
      assert(scope.resolveAs[Int](path = "y") contains 5)
      assert(scope.resolveAs[Double](path = "z") contains 12.0)
    }

    it("should return the value of the last statement") {
      val codeBlock = ChainedCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Plus("x".f, "y".f))
      val (scope, _, result) = LollypopVM.execute(Scope(), codeBlock)
      assert(result == 12.0)
    }

    it("should capture the value of a return statement") {
      val codeBlock = ChainedCodeBlock(
        SetAnyVariable(ref = "x".f, 7.v),
        SetAnyVariable(ref = "y".f, 5.v),
        Return(Some(Plus("x".f, "y".f))))
      assert(LollypopVM.execute(Scope(), codeBlock)._3 == 12.0)
    }

  }

}