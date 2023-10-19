package com.qwery.runtime.instructions.conditions

import com.qwery.QweryException
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import AssumeCondition.EnrichedAssumeCondition
import com.qwery.runtime.instructions.expressions.ArrayLiteral
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec


class AssumeConditionTest extends AnyFunSpec {

  describe(classOf[AssumeCondition].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeCondition(ArrayLiteral(1.v, 2.v, 3.v))
      assert(model.toSQL == "[1, 2, 3]")
    }

    it("should fail if the host instruction does not evaluate as a Condition") {
      assertThrows[QweryException] {
        val model = ArrayLiteral(1.v, 2.v, 3.v).asCondition
        QweryVM.execute(Scope(), model)
      }
    }

    it("should lazily evaluate a Condition") {
      val model = AssumeCondition(BooleanLiteral(true))
      assert(RuntimeCondition.isTrue(model)(Scope()))
    }

  }

}
