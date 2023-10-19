package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.ArrayLiteral
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import java.awt.Color

class ImportTest extends AnyFunSpec with VerificationTools {

  describe(classOf[Import].getSimpleName) {

    it("should compile an import statement") {
      val scope0 = Scope()
      val model = scope0.getCompiler.compile(
        """|import 'java.util.Date'
           |""".stripMargin)
      assert(model == Import("java.util.Date".v))
    }

    it("should compile an aliased import statement") {
      val scope0 = Scope()
      val model = scope0.getCompiler.compile(
        """|import JDate: 'java.util.Date'
           |""".stripMargin)
      assert(model == Import("java.util.Date".v.as("JDate")))
    }

    it("should compile an import statement with multiple classes") {
      val scope0 = Scope()
      val model = scope0.getCompiler.compile(
        """|import [
           |    'java.awt.Color',
           |    'java.awt.Dimension'
           |]
           |""".stripMargin)
      assert(model == Import(ArrayLiteral(
        "java.awt.Color".v, "java.awt.Dimension".v
      )))
    }

    it("should decompile an import statement") {
      val model = Import("java.util.Date".v)
      assert(model.toSQL == """import "java.util.Date"""")
    }

    it("should decompile an aliased import") {
      val model = Import("java.util.Date".v.as("JDate"))
      assert(model.toSQL == """import JDate: "java.util.Date"""")
    }

    it("should decompile an import statement with multiple classes") {
      val model = Import(ArrayLiteral("java.awt.Color".v, "java.awt.Dimension".v))
      assert(model.toSQL == """import ["java.awt.Color", "java.awt.Dimension"]""")
    }

    it("should execute an import statement") {
      val scope0 = Scope()
      val (_, _, result1) = QweryVM.executeSQL(scope0,
        """|import 'java.util.Date'
           |
           |set eventTime = new Date(1662753480123)
           |eventTime
           |""".stripMargin)
      assert(result1 == DateHelper.from(1662753480123L))
    }

    it("should execute an aliased import") {
      val scope0 = Scope()
      val (_, _, result1) = QweryVM.executeSQL(scope0,
        """|import JDate: 'java.util.Date'
           |
           |set eventTime = new JDate(1662753480123)
           |eventTime
           |""".stripMargin)
      assert(result1 == DateHelper.from(1662753480123L))
    }

    it("should execute an import statement with multiple classes") {
      val scope0 = Scope()
      val (_, _, result1) = QweryVM.executeSQL(scope0,
        """|import [
           |    'java.awt.Color',
           |    'java.awt.Dimension',
           |    'javax.swing.JFrame',
           |    'javax.swing.JPanel'
           |]
           |
           |set colors = [Color.GREEN, Color.MAGENTA, Color.CYAN, Color.BLUE]
           |colors[0]
           |""".stripMargin)
      assert(result1 == Color.GREEN)
    }

  }

}
