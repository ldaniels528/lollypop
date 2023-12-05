package com.lollypop.repl

import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL

class ProcessRunTest extends REPLFunSpec {

  describe(classOf[ProcessRun].getSimpleName) {

    it("should compile: (% iostat 1 5 %)") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|(% iostat 1 5 %)
           |""".stripMargin)
      assert(model == ProcessRun(" iostat 1 5 "))
    }

    it("should decompile: (% iostat 1 5 %)") {
      val model = ProcessRun(" iostat 1 5 ")
      assert(model.toSQL == "(% iostat 1 5 %)")
    }

    it("should execute: (% ps aux %) limit 5") {
      val (_, _, rc) =
        """|(% ps aux %) limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

  }

}
