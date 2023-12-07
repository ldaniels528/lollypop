package com.lollypop.runtime.instructions.conditions

import com.lollypop.LollypopException
import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.{Scope, _}
import org.scalatest.funspec.AnyFunSpec

import java.util.Date


class AssumeConditionTest extends AnyFunSpec {

  describe(classOf[AssumeCondition].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeCondition(ArrayLiteral(1.v, 2.v, 3.v))
      assert(model.toSQL == "[1, 2, 3]")
    }

    it("should fail if the host instruction does not evaluate as a Condition") {
      assertThrows[LollypopException] {
        val model = new Date().v.asCondition
        model.execute(Scope())
      }
    }

    it("should lazily evaluate a Condition") {
      val model = AssumeCondition(BooleanLiteral(true))
      assert(RuntimeCondition.isTrue(model)(Scope()))
    }

  }

}
