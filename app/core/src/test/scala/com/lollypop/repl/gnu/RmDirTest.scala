package com.lollypop.repl.gnu

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL

class RmDirTest extends REPLFunSpec {

  describe(classOf[RmDir].getSimpleName) {

    it("should compile: rmdir temp") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|rmdir temp
           |""".stripMargin)
      assert(model == RmDir("temp".f))
    }

    it("should compile: rmdir 'temp'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|rmdir 'temp'
           |""".stripMargin)
      assert(model == RmDir("temp".v))
    }

    it("should decompile: rmdir temp") {
      val model = RmDir("temp".f)
      assert(model.toSQL == """rmdir temp""")
    }

    it("should decompile: rmdir 'temp'") {
      val model = RmDir("temp".v)
      assert(model.toSQL == """rmdir "temp"""")
    }

    it("should execute: rmdir temp") {
      val (_, _, va) =
        """|created = mkdir temp_zzz
           |response = iff(not created, "Couldn't do it", "Done")
           |rmdir temp_zzz
           |response
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == "Done")
    }

  }

}
