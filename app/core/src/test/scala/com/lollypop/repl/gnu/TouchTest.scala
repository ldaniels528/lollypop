package com.lollypop.repl.gnu

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class TouchTest extends REPLFunSpec {

  describe(classOf[Touch].getSimpleName) {

    it("should compile: touch 'test.csv'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|touch 'test.csv'
           |""".stripMargin)
      assert(model == Touch("test.csv".v))
    }

    it("should decompile: touch 'test.csv'") {
      val model = Touch("test.csv".v)
      assert(model.toSQL == """touch "test.csv"""")
    }

    it("should execute: touch 'app/core/src/test/resources/lootBox.txt'") {
      val (_, _, va) =
        """|touch 'app/core/src/test/resources/lootBox.txt'
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == true)
    }

  }

}
