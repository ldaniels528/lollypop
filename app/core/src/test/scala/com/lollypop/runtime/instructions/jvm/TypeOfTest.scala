package com.lollypop.runtime.instructions.jvm

import com.lollypop.language._
import com.lollypop.language.models.$
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TypeOfTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[TypeOf].getSimpleName) {

    //////////////////////////////////////////////////////////////////////////////////
    //    EXPRESSION
    //////////////////////////////////////////////////////////////////////////////////

    it("should support being compiled") {
      val model = compiler.compile("select typeOf(x)")
      assert(model == Select(fields = Seq(TypeOf("x".f))))
    }

    it("should support being decompiled") {
      val model = TypeOf($("x"))
      assert(model.toSQL == "typeOf($x)")
    }

    it("should support being executed") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),sql =
        """|set x = new `java.util.Date`(1631508164812)
           |set typeName = typeOf(x)
           |typeName
           |""".stripMargin)
      assert(result == "java.util.Date")
    }

  }

}
