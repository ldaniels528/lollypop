package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.Expression.implicits._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.{$, @@@}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler}
import com.lollypop.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class CreateMacroTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateMacro].getSimpleName) {

    it("should compile create macro") {
      val results = compiler.compile(
        """|create macro `FETCH` := "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit"
           |select * from @@source where condition is true
           |""".stripMargin)
      assert(results ==
        CreateMacro(
          ref = DatabaseObjectRef("FETCH"),
          ifNotExists = false,
          `macro` = Macro(
            template = "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit",
            code = Select(fields = Seq("*".f), from = Some(@@@("source")), where = "condition".f is true)
          )))
    }

    it("should decompile create macro") {
      val model = CreateMacro(
        ref = DatabaseObjectRef("FETCH"),
        ifNotExists = false,
        `macro` = Macro(
          template = "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit",
          code = Select(fields = Seq("*".f), from = Some(@@@("source")), where = $("condition") is true)
        ))
      assert(model.toSQL ==
        """create macro FETCH := "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit" select * from @@source where $condition is true""")
    }

  }

}
