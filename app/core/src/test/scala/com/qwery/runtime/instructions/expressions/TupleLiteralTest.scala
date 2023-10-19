package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TupleLiteralTest extends AnyFunSpec {

  describe(classOf[TupleLiteral].getSimpleName) {

    it("should compile: (7, 1, 4, 3, 8, 2)") {
      assert(QweryCompiler().compile("(7, 1, 4, 3, 8, 2)") == TupleLiteral(List(7, 1, 4, 3, 8, 2).map(_.v)))
    }

    it("should decompile: (7, 1, 4, 3, 8, 2)") {
      assert(TupleLiteral(List(7, 1, 4, 3, 8, 2).map(_.v)).toSQL == "(7, 1, 4, 3, 8, 2)")
    }

    it("should execute: (7, 1, 4, 3, 8, 2)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|(7, 1, 4, 3, 8, 2)
           |""".stripMargin)
      assert(result == (7, 1, 4, 3, 8, 2))
    }

  }

}
