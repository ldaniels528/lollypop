package com.lollypop.repl.gnu

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.queryables.LollypopComponentsTest.LootBox

class CatTest extends REPLFunSpec {

  describe(classOf[Cat].getSimpleName) {

    it("should compile: cat 'test.csv'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cat 'test.csv'
           |""".stripMargin)
      assert(model == Cat("test.csv".v))
    }

    it("should decompile: cat 'test.csv'") {
      val model = Cat("test.csv".v)
      assert(model.toSQL == """cat "test.csv"""")
    }

    it("should execute: cat 'app/core/src/test/resources/lootBox.txt'") {
      val (_, _, ra) =
        """|cat 'app/core/src/test/resources/lootBox.txt'
           |""".stripMargin.searchSQL(createRootScope())
      assert(ra.toMapGraph == List(Map("cat" -> LootBox.getClass.getName)))
    }

  }

}
