package com.lollypop.runtime.instructions.expressions

import com.lollypop.LollypopException
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.instructions.expressions.AssumeExpression.EnrichedAssumeExpression
import com.lollypop.runtime.instructions.infrastructure.Drop
import com.lollypop.runtime.instructions.queryables.This
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import org.scalatest.funspec.AnyFunSpec

class AssumeExpressionTest extends AnyFunSpec {

  describe(classOf[AssumeExpression].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeExpression(Drop(DatabaseObjectRef("MyTable"), ifExists = false))
      assert(model.toSQL == "drop MyTable")
    }

    it("should fail if the host instruction does not evaluate as an Expression") {
      assertThrows[LollypopException] {
        val model = Drop(DatabaseObjectRef("MyTable"), ifExists = false).asExpression
        model.execute(Scope())
      }
    }

    it("should lazily evaluate an Expression") {
      val model = AssumeExpression(This())
      model.execute(Scope())
    }

  }

}
