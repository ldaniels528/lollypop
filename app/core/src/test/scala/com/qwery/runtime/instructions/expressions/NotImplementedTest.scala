package com.qwery.runtime.instructions.expressions

import com.qwery.QweryException
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class NotImplementedTest extends AnyFunSpec with VerificationTools {

  describe(classOf[NotImplemented].getSimpleName) {

    it("should fail if an non-implemented method is called") {
      assertThrows[QweryException] {
        QweryVM.executeSQL(Scope(),
          """|def blowUp() := ???
             |blowUp()
             |""".stripMargin)
      }
    }

  }

}
