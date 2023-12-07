package com.lollypop.runtime.instructions.invocables

import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ImportImplicitClassTest extends AnyFunSpec with VerificationTools {

  describe(classOf[ImportImplicitClass].getSimpleName) {

    it("should compile") {
      val model = LollypopCompiler().compile(
        """|import implicit "com.lollypop.util.StringRenderHelper$StringRenderer"
           |""".stripMargin)
      assert(model == ImportImplicitClass("com.lollypop.util.StringRenderHelper$StringRenderer".v))
    }

    it("should decompile") {
      val model = ImportImplicitClass("com.lollypop.util.StringRenderHelper$StringRenderer".v)
      assert(model.toSQL == "import implicit \"com.lollypop.util.StringRenderHelper$StringRenderer\"")
    }

    it("should execute Scala-native implicit classes") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|import implicit "com.lollypop.util.StringRenderHelper$StringRenderer"
           |"Hello".renderAsJson()
           |""".stripMargin)
      assert(result == "\"Hello\"")
    }

  }

}
