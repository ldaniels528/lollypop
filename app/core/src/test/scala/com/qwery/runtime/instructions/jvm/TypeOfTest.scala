package com.qwery.runtime.instructions.jvm

import com.qwery.language.models.@@
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class TypeOfTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[TypeOf].getSimpleName) {

    //////////////////////////////////////////////////////////////////////////////////
    //    EXPRESSION
    //////////////////////////////////////////////////////////////////////////////////

    it("should support being compiled") {
      val model = compiler.compile("select typeOf(@x)")
      assert(model == Select(fields = Seq(TypeOf(@@("x")))))
    }

    it("should support being decompiled") {
      val model = TypeOf(@@("x"))
      assert(model.toSQL == "typeOf(@x)")
    }

    it("should support being executed") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),sql =
        """|set x = new `java.util.Date`(1631508164812)
           |set typeName = typeOf(x)
           |typeName
           |""".stripMargin)
      assert(result == "java.util.Date")
    }

  }

}
