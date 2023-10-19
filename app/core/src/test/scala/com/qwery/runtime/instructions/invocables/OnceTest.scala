package com.qwery.runtime.instructions.invocables

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class OnceTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Once].getSimpleName) {

    it("should invoke an instruction only once") {
      implicit val scope: Scope = Scope()
      val (_, _, results) = QweryVM.executeSQL(Scope(),
        """|var n = 0
           |[1 to 5].foreach((m: Int) => {
           |  once {
           |    set n = n + 3
           |  }
           |  set n = n + 1
           |})
           |n
           |""".stripMargin)
      assert(results == 8)
    }

  }

}
