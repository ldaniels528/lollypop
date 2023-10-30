package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec

class IsDefinedTest extends AnyFunSpec {

  describe(classOf[IsDefined].getSimpleName) {

    it("resolves: should compile into a model") {
      assert(LollypopCompiler().compile("isDefined(x)") == IsDefined("x".f))
    }

    it("resolves: should decompile into SQL") {
      assert(IsDefined("x".f).toSQL == "isDefined(x)")
    }

    it("resolves: isDefined(y)")(isFalse("isDefined(y)"))

    it("resolves: isDefined(x)")(isTrue(
      """|set x = 1
         |isDefined(x)
         |""".stripMargin))

    it("resolves: isDefined(f)")(isTrue(
      """|def f(x: Int) := x + 1
         |isDefined(f)
         |""".stripMargin))

  }

  private def isTrue(sql: String): Assertion = {
    val result = LollypopVM.executeSQL(Scope(), sql)._3
    assert(result == true)
  }

  private def isFalse(sql: String): Assertion = {
    val result = LollypopVM.executeSQL(Scope(), sql)._3
    assert(result == false)
  }

}
