package com.lollypop.runtime.instructions.invocables

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DeclarePackageTest extends AnyFunSpec with VerificationTools {

  describe(classOf[DeclarePackage].getSimpleName) {

    it("should set the default package name in the scope") {
      val (_, _, rv) = LollypopVM.executeSQL(Scope(),
        """|package "com.acme.skunkworks"
           |__package__
           |""".stripMargin)
      assert(rv == "com.acme.skunkworks")
    }

  }

}
