package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.runtime.errors.ScenarioNotFoundError
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.conditions.Verify
import com.lollypop.runtime.instructions.expressions.{Dictionary, WWW, Infix}
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

/**
 * Scenario Test Suite
 */
class ScenarioTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Scenario].getSimpleName) {

    it("should compile a simple scenario") {
      val model = compiler.compile(
        """|scenario 'Create a new contest' {
           |    val response = www post 'http://{{host}}:{{port}}/api/shocktrade/contests' <~ { name: "Winter is coming" }
           |    verify response.statusCode is 200
           |}
           |""".stripMargin)
      assert(model == Scenario(
        title = "Create a new contest".v,
        verifications = Seq(
          ValVar(ref = "response", `type` = None, initialValue = Some(
            WWW(method = "post", url = "http://{{host}}:{{port}}/api/shocktrade/contests".v, body = Some(
              Dictionary(Map("name" -> "Winter is coming".v))
            ))
          ), isReadOnly = true),
          Verify(Infix("response".f, "statusCode".f) is 200.v)
        )
      ))
    }

    it("should compile a scenario that \"extends\" a parent scenario") {
      val model = compiler.compile(
        """|scenario 'Retrieve the previously created contest' extends 'Create a new contest' {
           |    val response = www get 'http://{{host}}:{{port}}/api/shocktrade/contests?id={{contest_id}}'
           |    verify response.statusCode is 200
           |}
           |""".stripMargin)
      assert(model == Scenario(
        title = "Retrieve the previously created contest".v,
        inherits = Some("Create a new contest".v),
        verifications = Seq(
          ValVar(ref = "response", `type` = None, initialValue = Some(
            WWW(method = "get", url = "http://{{host}}:{{port}}/api/shocktrade/contests?id={{contest_id}}".v)
          ), isReadOnly = true),
          Verify(Infix("response".f, "statusCode".f) is 200.v)
        )
      ))
    }

    it("should execute a scenario that \"extends\" its state from a peer scenario") {
      val (_, _, result) = LollypopVM.executeSQL(Scope(),
        """|feature "State Inheritance" {
           |  scenario 'Create a contest' {
           |    val contest_id = "40d1857b-474c-4400-8f07-5e04cbacc021"
           |    var counter = 1
           |    stdout <=== "contest_id = {{contest_id}}, counter = {{counter}}"
           |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
           |        and counter is 1
           |  }
           |
           |  scenario 'Create a member' {
           |    val member_id = "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
           |    stdout <=== "member_id = {{member_id}}"
           |    verify member_id is "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
           |  }
           |
           |  scenario 'Inherit contest state' extends 'Create a contest' {
           |    counter = counter + 1
           |    stdout <=== "contest_id = {{contest_id}}, counter = {{counter}}"
           |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
           |        and counter is 2
           |  }
           |
           |  scenario 'Inherit contest and member state' extends ['Create a contest', 'Create a member'] {
           |    counter = counter + 1
           |    stdout <=== "contest_id = {{contest_id}}, member_id = {{member_id}}, counter = {{counter}}"
           |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
           |        and member_id is "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
           |        and counter is 3
           |  }
           |}
           |""".stripMargin)
      assert(result == Map("passed" -> 4, "failed" -> 0))
    }

    it("should fail if the state inherited from a peer scenario does not exist") {
      assertThrows[ScenarioNotFoundError] {
        LollypopVM.executeSQL(Scope(),
          """|feature "Shared state between tests" {
             |  scenario 'Create some state' {
             |    contest_id = "40d1857b-474c-4400-8f07-5e04cbacc021"
             |    counter = 1
             |    stdout <=== "contest_id = {{contest_id}}, counter = {{counter}}"
             |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
             |        and counter is 1
             |  }
             |
             |  scenario 'Pass state from a parent' extends 'Create XXX state' {
             |    counter = counter + 1
             |    stdout <=== "contest_id = {{contest_id}}, counter = {{counter}}"
             |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
             |        and counter is 2
             |  }
             |}
             |""".stripMargin)
      }
    }

  }

}
