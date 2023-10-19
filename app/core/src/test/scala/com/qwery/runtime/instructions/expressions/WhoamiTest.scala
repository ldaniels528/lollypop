package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

import scala.util.Properties

class WhoamiTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Whoami].getSimpleName) {

    it("should compile: whoami") {
      val model = compiler.compile("whoami")
      assert(model == new Whoami())
    }

    it("should decompile: whoami") {
      val model = new Whoami()
      assert(model.toSQL == "whoami")
    }

    it("should return the name of the current user") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|whoami
           |""".stripMargin)
      assert(result == Properties.userName)
    }

  }

}