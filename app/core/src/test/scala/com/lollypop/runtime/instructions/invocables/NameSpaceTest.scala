package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NamespaceTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Namespace].getSimpleName) {

    it("should support compilation") {
      val results = compiler.compile("namespace 'acme'")
      assert(results == Namespace(expression = "acme".v))
    }

    it("should support de-compilation") {
      val model = Namespace(expression = "acme".v)
      assert(model.toSQL == "use \"acme\"")
    }

    it("should set the default database") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),"namespace 'acme'")
      assert(scope.getDatabase contains "acme")
    }

    it("should set the default database and schema") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),"namespace 'ldaniels.work'")
      assert(scope.getDatabase contains "ldaniels")
      assert(scope.getSchema contains "work")
    }

  }

}
