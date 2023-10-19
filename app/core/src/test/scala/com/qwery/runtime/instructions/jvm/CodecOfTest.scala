package com.qwery.runtime.instructions.jvm

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CodecOfTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[CodecOf].getSimpleName) {

    //////////////////////////////////////////////////////////////////////////////////
    //    EXPRESSION
    //////////////////////////////////////////////////////////////////////////////////

    it("should support being compiled") {
      val model = compiler.compile("select codecOf(x)")
      assert(model == Select(fields = Seq(CodecOf("x".f))))
    }

    it("should support being decompiled") {
      val model = CodecOf("x".f)
      assert(model.toSQL == "codecOf(x)")
    }

    it("should support being executed") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),sql =
        """|set x = new `java.util.Date`(1631508164812)
           |codecOf(x)
           |""".stripMargin)
      assert(result == "DateTime")
    }

  }

}
