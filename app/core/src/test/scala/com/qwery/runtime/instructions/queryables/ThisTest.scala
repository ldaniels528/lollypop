package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.expressions.Infix
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ThisTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(This.getClass.getSimpleName) {

    ////////////////////////////////////////////////////////////////////////
    //    EXPRESSIONS
    ////////////////////////////////////////////////////////////////////////

    it("should compile (expression)") {
      val results = compiler.compile("select this")
      assert(results == Select(fields = Seq(This())))
    }

    it("should decompile (expression)") {
      verify("select this")
    }

    it("should execute (expression)") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|set @n = 123
            |select x: this.toTable()
            |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph.filterNot(_.exists {
        case ("name", "π") => true
        case ("name", "stdout") => true
        case ("name", "stderr") => true
        case ("name", "stdin") => true
        case ("name", "OS") => true
        case ("name", "Random") => true
        case _ => false
      }) == List(
        Map("name" -> "n", "value" -> "123", "kind" -> "Integer")
      ))
    }

    ////////////////////////////////////////////////////////////////////////
    //    QUERYABLES
    ////////////////////////////////////////////////////////////////////////

    it("should compile (queryable)") {
      val results = compiler.compile("from this.toTable() where name == '@x'")
      assert(results == Select(fields = Seq("*".f), from = Some(From(Infix(This(), "toTable".fx()))), where = Some("name".f === "@x".v)))
    }

    it("should decompile (queryable)") {
      verify("from (this.toTable()) where name == '@x'")
    }

    it("should execute (queryable)") {
      val (_, _, device) = QweryVM.searchSQL(Scope(), sql =
        s"""|set @n = 123
            |select value from (this.toTable()) where name is "n"
            |""".stripMargin)
      assert(device.tabulate() == List(
        "|-------|",
        "| value |",
        "|-------|",
        "| 123   |",
        "|-------|"
      ))
    }

  }

}
