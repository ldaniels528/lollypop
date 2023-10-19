package com.qwery.runtime.instructions.queryables

import com.qwery.QweryException
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import AssumeQueryable.EnrichedAssumeQueryable
import com.qwery.runtime.instructions.expressions.ArrayLiteral
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AssumeQueryableTest extends AnyFunSpec {

  describe(classOf[AssumeQueryable].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeQueryable(ArrayLiteral(1.v, 2.v, 3.v))
      assert(model.toSQL == "[1, 2, 3]")
    }

    it("should fail if the host instruction does not evaluate as a Queryable") {
      assertThrows[QweryException] {
        val model = ArrayLiteral(1.v, 2.v, 3.v).asQueryable
        QweryVM.search(Scope(), model)
      }
    }

    it("should lazily evaluate a Queryable") {
      val model = AssumeQueryable(This())
      QweryVM.search(Scope(), model)
    }

  }

}
