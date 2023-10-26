package com.qwery.runtime.instructions.invocables

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DeclarePackageTest extends AnyFunSpec with VerificationTools {

  describe(classOf[DeclarePackage].getSimpleName) {

    it("should set the default package name in the scope") {
      val (_, _, rv) = QweryVM.executeSQL(Scope(),
        """|package "com.acme.skunkworks"
           |__package__
           |""".stripMargin)
      assert(rv == "com.acme.skunkworks")
    }

  }

}
