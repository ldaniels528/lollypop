package com.qwery.runtime.instructions.invocables

import com.qwery.language.Template
import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.ProcedureCall
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class WhenEverTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[WhenEver].getSimpleName) {

    it("should parse: whenever lives == 0 ...") {
      val params = Template(WhenEver.templateCard).processWithDebug(
        """|whenever lives == 0 call gameOver()
           |""".stripMargin)
      assert(params.all == Map(
        "expr" -> ("lives".f === 0),
        "code" -> ProcedureCall(ref = DatabaseObjectRef("gameOver"), args = Nil),
        "keywords" -> Set("whenever")
      ))
    }

    it("should compile: whenever lives == 0 ...") {
      val results = compiler.compile(
        """|whenever lives == 0 call gameOver()
           |""".stripMargin)
      assert(results == WhenEver("lives".f === 0, ProcedureCall(ref = DatabaseObjectRef("gameOver"), args = Nil)))
    }

    it("should decompile: whenever lives == 0 ...") {
      verify(
        """|whenever `lives` == 0 call gameOver()
           |""".stripMargin)
    }

    it("should execute: whenever lives == 0 ...") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        s"""|var deaths: Int = 0
            |var health: Int = 1
            |whenever health == 0 set deaths = deaths + 1
            |out.println("health: {{health}}  deaths: {{deaths}}")
            |set health = health - 1
            |out.println("health: {{health}}  deaths: {{deaths}}")
            |""".stripMargin)
      assert(scope.resolve("deaths") contains 1)
      assert(scope.resolve("health") contains 0)
    }

    it("should execute: whenever '^out.println(.*)' ...") {
      val (scope, _, _) = QweryVM.executeSQL(Scope(),
        s"""|var counter = 0
            |whenever '^out.println(.*)' {
            |   counter += 1
            |}
            |out.println('Hello World 1')
            |out.println('Hello World 2')
            |""".stripMargin)
      assert(scope.resolve("counter") contains 2)
    }

  }

}
