package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.repl.Ls
import com.lollypop.runtime.LollypopVM.implicits.LollypopVMSQL
import com.lollypop.runtime.Scope
import org.scalatest.funspec.AnyFunSpec

class LsTest extends AnyFunSpec {

  describe(classOf[Ls].getSimpleName) {

    it("should compile: ls './app/examples/'") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls './app/examples/'")
      assert(model == Ls(Some("./app/examples/".v)))
    }

    it("should compile: ls -('./app/examples/')") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls -('./app/examples/')")
      assert(model == Ls(Some(NEG("./app/examples/".v))))
    }

    it("should compile: ls ~('./app/examples/')") {
      val compiler = createRootScope().getCompiler
      val model = compiler.compile("ls ~('./app/examples/')")
      assert(model == Ls(Some(Tilde("./app/examples/".v))))
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

    it("should execute: ls -('./app/examples/')") {
      val (_, _, rc) =
        """|select name
           |from (ls -("./app/examples/"))
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

    it("should execute: ls ~('.')") {
      val (_, _, rc) =
        """|select name
           |from (ls ~("."))
           |limit 5
           |""".stripMargin.searchSQL(createRootScope())
      rc.tabulate().foreach(println)
    }

  }

  private def createRootScope(): Scope = {
    s"""|lollypopComponents("${Ls.getClass.getName}")
        |""".stripMargin.executeSQL(Scope())._1
  }

}
