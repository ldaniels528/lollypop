package com.lollypop.runtime.instructions.queryables

import com.lollypop.LollypopException
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import AssumeQueryable.EnrichedAssumeQueryable
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class AssumeQueryableTest extends AnyFunSpec {

  describe(classOf[AssumeQueryable].getSimpleName) {

    it("should provide the toSQL of the host instruction") {
      val model = AssumeQueryable(ArrayLiteral(1.v, 2.v, 3.v))
      assert(model.toSQL == "[1, 2, 3]")
    }

    it("should fail if the host instruction does not evaluate as a Queryable") {
      assertThrows[LollypopException] {
        val model = ArrayLiteral(1.v, 2.v, 3.v).asQueryable
        LollypopVM.search(Scope(), model)
      }
    }

    it("should lazily evaluate a Queryable") {
      val model = AssumeQueryable(This())
      LollypopVM.search(Scope(), model)
    }

  }

}
