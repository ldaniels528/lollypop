package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NamespaceTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

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
      val (scope, _, _) = QweryVM.executeSQL(Scope(),"namespace 'acme'")
      assert(scope.getDatabase contains "acme")
    }

    it("should set the default database and schema") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),"namespace 'ldaniels.work'")
      assert(scope.getDatabase contains "ldaniels")
      assert(scope.getSchema contains "work")
    }

  }

}
