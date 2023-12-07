package com.lollypop.repl.gnu

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class RemoveTest extends REPLFunSpec {

  describe(classOf[Remove].getSimpleName) {

    it("should compile: rm 'test.csv'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|rm 'test.csv'
           |""".stripMargin)
      assert(model == Remove("test.csv".v))
    }

    it("should decompile: rm 'test.csv'") {
      val model = Remove("test.csv".v)
      assert(model.toSQL == """rm "test.csv"""")
    }

    it("should execute: rm 'app/core/src/test/resources/lootBox.txt'") {
      val (_, _, va) =
        """|cp 'build.sbt' 'temp.txt'
           |rm 'temp.txt' 
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == true)
    }

  }

}
