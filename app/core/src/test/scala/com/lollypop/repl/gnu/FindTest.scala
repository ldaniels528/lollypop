package com.lollypop.repl.gnu

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.repl.REPLFunSpec
import com.lollypop.repl.symbols.Tilde
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.instructions.operators.Div

class FindTest extends REPLFunSpec {

  describe(classOf[Find].getSimpleName) {

    it("should compile: find app/examples") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("find app/examples")
      assert(model == Find(Div("app".f, "examples".f)))
    }

    it("should compile: find _/app/examples") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("find _/app/examples")
      assert(model == Find(Div("_".f, Div("app".f, "examples".f))))
    }

    it("should compile: find ~/Documents") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("find ~/Documents")
      assert(model == Find(Div(Tilde(), "Documents".f)))
    }

    it("should compile: find './app/examples/'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("find './app/examples/'")
      assert(model == Find("./app/examples/".v))
    }

    it("should compile: find '~/app/examples/'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("find '~/app/examples/'")
      assert(model == Find("~/app/examples/".v))
    }

    it("should execute: find app/examples") {
      val (_, _, rc) =
        """|select name
           |from (find app/examples)
           |where name matches "(.*)[.]csv"
           |limit 5
           |""".stripMargin.searchSQL(createRootScope())
      assert(rc.toMapGraph.toSet == Set(
        Map("name" -> "stocks-100k.csv"),
        Map("name" -> "companylist-amex.csv"),
        Map("name" -> "companylist-nyse.csv"),
        Map("name" -> "companylist-nasdaq.csv"),
        Map("name" -> "stocks-5k.csv")
      ))
    }

    it("should execute: find './app/examples/'") {
      val (_, _, rc) =
        """|select name
           |from (find "./app/examples/")
           |where name matches "(.*)[.]csv"
           |""".stripMargin.searchSQL(createRootScope())
      assert(rc.toMapGraph.toSet == Set(
        Map("name" -> "stocks-100k.csv"),
        Map("name" -> "companylist-amex.csv"),
        Map("name" -> "companylist-nyse.csv"),
        Map("name" -> "companylist-nasdaq.csv"),
        Map("name" -> "stocks-5k.csv"),
        Map("name" -> "stocks.csv")
      ))
    }

    it("should execute: find app/examples limit 5") {
      val (_, _, rc) =
        """|find app/examples limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

    it("should execute: find 'app/examples' limit 5") {
      val (_, _, rc) =
        """|find 'app/examples' limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

  }

}
