package com.lollypop.runtime.instructions.invocables

import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime.LollypopVMSQL

class ProcessExecTest extends REPLFunSpec {

  describe(classOf[ProcessExec].getSimpleName) {

    it("should compile: (? iostat 1 5 ?)") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|(? iostat 1 5 ?)
           |""".stripMargin)
      assert(model == ProcessExec(" iostat 1 5 "))
    }

    it("should decompile: (? iostat 1 5 ?)") {
      val model = ProcessExec(" iostat 1 5 ")
      assert(model.toSQL == "(? iostat 1 5 ?)")
    }

    it("should execute: (? iostat 1 5 ?)") {
      val (_, _, rc) =
        """|transpose(line: (? iostat 1 5 ?))
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

  }

}