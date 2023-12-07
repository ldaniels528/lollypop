package com.lollypop.repl.gnu

import com.lollypop.language.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class MkDirTest extends REPLFunSpec {

  describe(classOf[MkDir].getSimpleName) {

    it("should compile: mkdir temp") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|mkdir temp
           |""".stripMargin)
      assert(model == MkDir("temp".f))
    }

    it("should compile: mkdir 'temp'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|mkdir 'temp'
           |""".stripMargin)
      assert(model == MkDir("temp".v))
    }

    it("should decompile: mkdir temp") {
      val model = MkDir("temp".f)
      assert(model.toSQL == """mkdir temp""")
    }

    it("should decompile: mkdir 'temp'") {
      val model = MkDir("temp".v)
      assert(model.toSQL == """mkdir "temp"""")
    }

    it("should execute: mkdir temp") {
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
