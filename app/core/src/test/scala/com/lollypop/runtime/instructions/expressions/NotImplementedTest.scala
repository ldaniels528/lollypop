package com.lollypop.runtime.instructions.expressions

import com.lollypop.LollypopException
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NotImplementedTest extends AnyFunSpec with VerificationTools {

  describe(classOf[NotImplemented].getSimpleName) {

    it("should fail if an non-implemented method is called") {
      assertThrows[LollypopException] {
        LollypopVM.executeSQL(Scope(),
          """|def blowUp() := ???
             |blowUp()
             |""".stripMargin)
      }
    }

  }

}
