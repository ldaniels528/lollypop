package com.lollypop.repl.gnu

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class CopyTest extends REPLFunSpec {

  describe(classOf[Copy].getSimpleName) {

    it("should compile: cp 'test.csv' 'test1.csv'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|cp 'test.csv' 'test1.csv'
           |""".stripMargin)
      assert(model == Copy(source = "test.csv".v, target = "test1.csv".v))
    }

    it("should decompile: cp 'test.csv' 'test1.csv'") {
      val model = Copy(source = "test.csv".v, target = "test1.csv".v)
      assert(model.toSQL == """cp "test.csv" "test1.csv"""")
    }

    it("should execute: cp 'build.sbt' 'temp.txt'") {
      val (_, _, va) =
        """|try
           |  cp 'build.sbt' 'temp.txt'
           |catch e => -1
           |finally rm 'temp.txt'
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == 6223)
    }

  }

}
