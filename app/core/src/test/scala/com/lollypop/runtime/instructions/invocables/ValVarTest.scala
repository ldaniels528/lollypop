package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.runtime.errors.VariableIsReadOnlyError
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ValVarTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

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
        LollypopVM.executeSQL(Scope(),
          """|val counter: Int = 9
             |counter += 1
             |""".stripMargin)
      }
    }

    it("should define a mutable variable") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
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
