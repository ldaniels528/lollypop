package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{LollypopCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec

class VerifyTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[Verify].getSimpleName) {

    it("should interpret 'verify status is 200'") {
      val scope0 = Scope()
        .withVariable(name = "status", code = 200.v, isReadOnly = false)
      val verify = Verify(EQ("status".f, 200.v))
      val (_, _, result) = verify.execute(scope0)
      assert(result == true)
    }

    it("should interpret 'verify status isnt 200'") {
      val scope0 = Scope()
        .withVariable(name = "status", code = 200.v, isReadOnly = false)
      val verify = Verify(NEQ("status".f, 200.v))
      val (_, _, result) = verify.execute(scope0)
      assert(result == false)
    }

    it("should compile: verify status is 200 ^^^ 'Notebook created'") {
      val model = compiler.compile(
        """|verify statusCode is 200
           |  ^^^ "Notebook created"
           |""".stripMargin)
      assert(model == Verify(condition = Is("statusCode".f, 200.v), message = Some("Notebook created".v)))
    }

    it("should compile a complete scenario") {
      compiler.compile(
        """|scenario 'Create a new notebook' {
           |  val responseA = www post 'http://{{host}}:{{port}}/api/notebooks/notebooks' <~ { name: "ShockTrade" }
           |  val notebook_id = responseA.body.id
           |  verify responseA.statusCode is 200
           |    ^^^ "Notebook created"
           |}
           |""".stripMargin)
    }

  }

}
