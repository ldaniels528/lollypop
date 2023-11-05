package com.lollypop.runtime

import com.lollypop.runtime.LollypopCodeDebugger.createAutomatedConsoleReader
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import scala.language.implicitConversions

class LollypopCodeDebuggerTest extends AnyFunSpec {
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[LollypopCodeDebugger].getSimpleName) {

    it("should show the code at the current position") {
      val reader = createAutomatedConsoleReader("s")
      LollypopCodeDebugger().stepThrough(file = new File("./app/examples/src/main/lollypop/BreakOutDemo.sql"), console = reader)
    }

    it("should show a page of code lines") {
      val reader = createAutomatedConsoleReader("p")
      LollypopCodeDebugger().stepThrough(file = "./app/examples/src/main/lollypop/BreakOutDemo.sql", console = reader)
    }

    it("should show the list of code lines") {
      val reader = createAutomatedConsoleReader("l")
      LollypopCodeDebugger().stepThrough(file = "./app/examples/src/main/lollypop/BreakOutDemo.sql", console = reader)
    }

    it("should execute lines of code and show the scope") {
      val reader = createAutomatedConsoleReader("", "", "", "", "r", "", "?")
      LollypopCodeDebugger().stepThrough(file = "./app/examples/src/main/lollypop/BreakOutDemo.sql", console = reader)
    }

  }

}
