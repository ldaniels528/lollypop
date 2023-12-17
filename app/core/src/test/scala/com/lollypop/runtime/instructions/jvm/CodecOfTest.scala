package com.lollypop.runtime.instructions.jvm

import com.lollypop.language._
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class CodecOfTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

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
      val (_, _, result) = LollypopVM.executeSQL(Scope(),sql =
        """|set x = new `java.util.Date`(1631508164812)
           |codecOf(x)
           |""".stripMargin)
      assert(result == "DateTime")
    }

  }

}
