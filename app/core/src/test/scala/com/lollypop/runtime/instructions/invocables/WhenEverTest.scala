package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.Template
import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.ProcedureCall
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class WhenEverTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

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
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        s"""|var deaths: Int = 0
            |var health: Int = 1
            |whenever health == 0 set deaths = deaths + 1
            |stdout <=== "health: {{health}}  deaths: {{deaths}}"
            |set health = health - 1
            |stdout <=== "health: {{health}}  deaths: {{deaths}}"
            |""".stripMargin)
      assert(scope.resolve("deaths") contains 1)
      assert(scope.resolve("health") contains 0)
    }

    it("should execute: whenever '^stdout.println(.*)' ...") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        s"""|var counter = 0
            |whenever '^stdout.println(.*)' {
            |   counter += 1
            |}
            |stdout.println('Hello World 1')
            |stdout.println('Hello World 2')
            |""".stripMargin)
      assert(scope.resolve("counter") contains 2)
    }

  }

}
