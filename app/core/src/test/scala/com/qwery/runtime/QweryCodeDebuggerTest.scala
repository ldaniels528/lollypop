package com.qwery.runtime

import com.qwery.runtime.QweryCodeDebugger.createAutomatedConsoleReader
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import scala.language.implicitConversions

class QweryCodeDebuggerTest extends AnyFunSpec {
  private implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[QweryCodeDebugger].getSimpleName) {

    it("should show the code at the current position") {
      val reader = createAutomatedConsoleReader("s")
      QweryCodeDebugger().stepThrough(file = new File("./contrib/examples/src/main/qwery/BreakOutDemo.sql"), console = reader)
    }

    it("should show a page of code lines") {
      val reader = createAutomatedConsoleReader("p")
      QweryCodeDebugger().stepThrough(file = "./contrib/examples/src/main/qwery/BreakOutDemo.sql", console = reader)
    }

    it("should show the list of code lines") {
      val reader = createAutomatedConsoleReader("l")
      QweryCodeDebugger().stepThrough(file = "./contrib/examples/src/main/qwery/BreakOutDemo.sql", console = reader)
    }

    it("should execute lines of code and show the scope") {
      val reader = createAutomatedConsoleReader("", "", "", "", "r", "", "?")
      QweryCodeDebugger().stepThrough(file = "./contrib/examples/src/main/qwery/BreakOutDemo.sql", console = reader)
    }

  }

}
