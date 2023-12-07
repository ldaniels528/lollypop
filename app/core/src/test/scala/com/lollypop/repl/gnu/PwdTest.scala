package com.lollypop.repl.gnu

import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class PwdTest extends REPLFunSpec {

  describe(classOf[Pwd].getSimpleName) {

    it("should compile: pwd") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|pwd
           |""".stripMargin)
      assert(model == Pwd())
    }

    it("should decompile: pwd") {
      val model = Pwd()
      assert(model.toSQL == "pwd")
    }

    it("should execute: pwd") {
      val (_, _, va) =
        """|cd "/tmp"
           |pwd
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == "/tmp")
    }

  }

}

