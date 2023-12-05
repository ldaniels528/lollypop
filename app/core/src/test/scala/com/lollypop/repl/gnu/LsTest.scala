package com.lollypop.repl.gnu

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.repl.REPLFunSpec
import com.lollypop.repl.symbols.{Caret, Tilde}
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.instructions.operators.Div

class LsTest extends REPLFunSpec {

  describe(classOf[Ls].getSimpleName) {

    it("should compile: ls app/examples") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls app/examples")
      assert(model == Ls(Some(Div("app".f, "examples".f))))
    }

    it("should compile: ls ^/app/examples") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls ^/app/examples")
      assert(model == Ls(Some(Div(Caret(), Div("app".f, "examples".f)))))
    }

    it("should compile: ls ~/app/examples") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls ~/app/examples")
      assert(model == Ls(Some(Div(Tilde(), Div("app".f, "examples".f)))))
    }

    it("should compile: ls './app/examples/'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls './app/examples/'")
      assert(model == Ls(Some("./app/examples/".v)))
    }

    it("should compile: ls '~/app/examples/'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls '~/app/examples/'")
      assert(model == Ls(Some("~/app/examples/".v)))
    }

    it("should execute: ls app/examples") {
      val (_, _, rc) =
        """|select name
           |from (ls app/examples)
           |where name matches "(.*)[.]csv"
           |""".stripMargin.searchSQL(createRootScope())
      assert(rc.toMapGraph.toSet == Set(
        Map("name" -> "stocks-100k.csv"),
        Map("name" -> "stocks-5k.csv"),
        Map("name" -> "stocks.csv")
      ))
    }

    it("should execute: ls './app/examples/'") {
      val (_, _, rc) =
        """|select name
           |from (ls "./app/examples/")
           |where name matches "(.*)[.]csv"
           |""".stripMargin.searchSQL(createRootScope())
      assert(rc.toMapGraph.toSet == Set(
        Map("name" -> "stocks-100k.csv"),
        Map("name" -> "stocks-5k.csv"),
        Map("name" -> "stocks.csv")
      ))
    }

    it("should execute: ls ~") {
      val (_, _, rc) =
        """|select name from (ls ~) limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

    it("should execute: ls '~/'") {
      val (_, _, rc) =
        """|select name from (ls '~/') limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

  }

}
