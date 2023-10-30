package com.lollypop.runtime.instructions.invocables

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class OnceTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Once].getSimpleName) {

    it("should invoke an instruction only once") {
      implicit val scope: Scope = Scope()
      val (_, _, results) = LollypopVM.executeSQL(Scope(),
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
