package com.lollypop.repl.gnu

import com.lollypop.language.LifestyleExpressionsAny
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime._

class MoveTest extends REPLFunSpec {

  describe(classOf[Move].getSimpleName) {

    it("should compile: mv 'test.csv' 'test1.csv'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile(
        """|mv 'test.csv' 'test1.csv'
           |""".stripMargin)
      assert(model == Move(source = "test.csv".v, target = "test1.csv".v))
    }

    it("should decompile: mv 'test.csv' 'test1.csv'") {
      val model = Move(source = "test.csv".v, target = "test1.csv".v)
      assert(model.toSQL == """mv "test.csv" "test1.csv"""")
    }

    it("should execute: mv 'build.sbt' 'temp.txt'") {
      val (_, _, va) =
        """|try {
           |  cp 'build.sbt' 'temp.txt'
           |  mv 'temp.txt' 'temp1.txt'
           |} catch e => {
           |  e ===> stderr
           |  false
           |} finally rm 'temp.txt' or rm 'temp1.txt'
           |""".stripMargin.executeSQL(createRootScope())
      assert(va == true)
    }

  }

}
