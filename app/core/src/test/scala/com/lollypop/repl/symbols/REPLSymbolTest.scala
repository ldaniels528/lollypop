package com.lollypop.repl.symbols

import com.lollypop.language._
import com.lollypop.repl.REPLFunSpec
import com.lollypop.runtime.LollypopCompiler
import com.lollypop.runtime.instructions.invocables.SetVariableExpression
import com.lollypop.runtime.instructions.operators.{Amp, Div}

class REPLSymbolTest extends REPLFunSpec {

  describe(classOf[REPLSymbol].getSimpleName) {

    it("should compile: .") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile(".")
      assert(model == Dot())
    }

    it("should compile: ..") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile("..")
      assert(model == DotDot())
    }

    it("should compile: ~") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile("~")
      assert(model == Tilde())
    }

    it("should compile: ~/Downloads") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile("~/Downloads")
      assert(model == Div(Tilde(), "Downloads".f))
    }

    it("should compile: ^/Downloads") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile("^/Downloads")
      assert(model == Div(Caret(), "Downloads".f))
    }

    it("should compile: https://localhost/api?symbol=ABC&key=value") {
      val compiler: LollypopCompiler = createRootScope().getCompiler
      val model = compiler.compile("https://localhost/api?symbol=ABC&key=value")
      assert(model == ColonSlashSlash(a = "https".f, b = Div("localhost".f,
        QuestionMark("api".f, SetVariableExpression("symbol".f,
          Amp("ABC".f, SetVariableExpression("key".f, "value".f))))
      )))
    }

  }

}
