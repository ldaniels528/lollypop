package com.qwery.runtime.instructions.expressions

import com.qwery.QweryException
import AssumeExpression.EnrichedAssumeExpression
import com.qwery.runtime.instructions.infrastructure.Drop
import com.qwery.runtime.instructions.queryables.This
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AssumeExpressionTest extends AnyFunSpec {

  describe(classOf[AssumeExpression].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeExpression(Drop(DatabaseObjectRef("MyTable"), ifExists = false))
      assert(model.toSQL == "drop MyTable")
    }

    it("should fail if the host instruction does not evaluate as an Expression") {
      assertThrows[QweryException] {
        val model = Drop(DatabaseObjectRef("MyTable"), ifExists = false).asExpression
        QweryVM.execute(Scope(), model)
      }
    }

    it("should lazily evaluate an Expression") {
      val model = AssumeExpression(This())
      QweryVM.execute(Scope(), model)
    }

  }

}
