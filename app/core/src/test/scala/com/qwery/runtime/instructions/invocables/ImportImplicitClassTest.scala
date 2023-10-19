package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ImportImplicitClassTest extends AnyFunSpec with VerificationTools {

  describe(classOf[ImportImplicitClass].getSimpleName) {

    it("should compile") {
      val model = QweryCompiler().compile(
        """|import implicit "com.qwery.util.StringRenderHelper$StringRenderer"
           |""".stripMargin)
      assert(model == ImportImplicitClass("com.qwery.util.StringRenderHelper$StringRenderer".v))
    }

    it("should decompile") {
      val model = ImportImplicitClass("com.qwery.util.StringRenderHelper$StringRenderer".v)
      assert(model.toSQL == "import implicit \"com.qwery.util.StringRenderHelper$StringRenderer\"")
    }

    it("should execute Scala-native implicit classes") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|import implicit "com.qwery.util.StringRenderHelper$StringRenderer"
           |"Hello".renderAsJson()
           |""".stripMargin)
      assert(result == "\"Hello\"")
    }

  }

}
