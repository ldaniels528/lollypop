package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.errors.VariableIsReadOnlyError
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ValVarTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[ValVar].getSimpleName) {

    it("should support compiling VAL with an initial value") {
      val results = compiler.compile("val customerId: Int = 5")
      assert(results == ValVar(ref = "customerId", `type` = Some("Int".ct), initialValue = Some(5), isReadOnly = true))
    }

    it("should support compiling VAR with an initial value") {
      val results = compiler.compile("var customerId: String = '53421'")
      assert(results == ValVar(ref = "customerId", `type` = Some("String".ct), initialValue = Some("53421")))
    }

    it("should support decompiling VAL with an initial value") {
      verify("val customerId: integer = 5")
    }

    it("should support decompiling VAR with an initial value") {
      verify("var customerId: integer = 5")
    }

    it("should define an immutable (read-only) variable") {
      assertThrows[VariableIsReadOnlyError] {
        QweryVM.executeSQL(Scope(),
          """|val counter: Int = 9
             |counter += 1
             |""".stripMargin)
      }
    }

    it("should define a mutable variable") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|var counter: Int = 0
           |set counter = 5
           |counter += 6
           |counter -= 1
           |counter
           |""".stripMargin)
      assert(result == 10)
    }

  }

}
