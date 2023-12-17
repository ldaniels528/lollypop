package com.lollypop.repl

import com.lollypop.runtime.{Scope, _}
import org.scalatest.funspec.AnyFunSpec

class REPLFunSpec extends AnyFunSpec {

  def createRootScope(): Scope = {
    val parsers = LollypopREPL.languageParsers
    val sql =
      s"lollypopComponents('''${
        parsers.map(p => s"|${p.getClass.getName}").mkString("\n")
      }'''.stripMargin('|'))"
    sql.executeSQL(Scope())._1
  }

}
