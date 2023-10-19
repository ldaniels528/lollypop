package com.qwery.runtime.instructions.invocables

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class RequireTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Require].getSimpleName) {

    it("should support download required resources") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|require [
           |    "com.google.api-client:google-api-client:2.0.0"
           |]
           |
           |import "com.google.api.client.googleapis.notifications.TypedNotification"
           |@TypedNotification
           |""".stripMargin)
      assert(Option(result).collect { case c: Class[_] => c.getName } contains "com.google.api.client.googleapis.notifications.TypedNotification")
    }

  }

}
