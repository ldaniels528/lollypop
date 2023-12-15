package com.lollypop.repl

import com.lollypop.runtime.LollypopVMSQL

class ProcessPuppetTest extends REPLFunSpec {

  describe(classOf[ProcessPuppet].getSimpleName) {

    it("should compile: (& iostat 1 5 &)") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|(& iostat 1 5 &)
           |""".stripMargin)
      assert(model == ProcessPuppet(" iostat 1 5 "))
    }

    it("should decompile: (& iostat 1 5 &)") {
      val model = ProcessPuppet(" iostat 1 5 ")
      assert(model.toSQL == "(& iostat 1 5 &)")
    }

    it("should execute: (& ps aux &)") {
      val (_, _, va) =
        """|p = (& hostname &)
           |p.exitValue()
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == 0)
    }

  }

}
