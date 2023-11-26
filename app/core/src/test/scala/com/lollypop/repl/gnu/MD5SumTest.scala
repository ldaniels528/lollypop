package com.lollypop.repl.gnu

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL

class MD5SumTest extends REPLFunSpec {

  describe(classOf[MD5Sum].getSimpleName) {

    it("should compile: md5 'BadPassword123'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|md5 'BadPassword123'
           |""".stripMargin)
      assert(model == MD5Sum("BadPassword123".v))
    }

    it("should decompile: md5 'BadPassword123'") {
      val model = MD5Sum("BadPassword123".v)
      assert(model.toSQL == """md5 "BadPassword123"""")
    }

    it("should execute: md5 'BadPassword123'") {
      val (_, _, va: Array[Byte]) =
        """|md5 "BadPassword123"
           |""".stripMargin.executeSQL(createRootScope())
      info(va.mkString(", "))
      assert(va sameElements Array[Byte](-32, -18, -78, 52, 112, -115, 96, 68, 86, -38, -103, -1, -70, 7, -6, 41))
    }

    it("should execute: md5 new `java.io.File`('...')") {
      val (_, _, va: Array[Byte]) =
        """|md5(new `java.io.File`("app/core/src/main/scala/com/lollypop/repl/gnu/MD5Sum.scala"))
           |""".stripMargin.executeSQL(createRootScope())
      info(va.mkString(", "))
      assert(va sameElements Array[Byte](65, 89, 34, 29, 55, 71, -127, -37, 26, -64, 47, 39, -22, -51, -72, 49))
    }

  }

}
