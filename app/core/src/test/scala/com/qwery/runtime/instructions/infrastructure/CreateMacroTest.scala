package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.language.models.{@@, @@@}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{DatabaseObjectRef, QweryCompiler}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class CreateMacroTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[CreateMacro].getSimpleName) {

    it("should compile create macro") {
      val results = compiler.compile(
        """|create macro `FETCH` := "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit"
           |select * from @@source where @condition is true
           |""".stripMargin)
      assert(results ==
        CreateMacro(
          ref = DatabaseObjectRef("FETCH"),
          ifNotExists = false,
          `macro` = Macro(
            template = "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit",
            code = Select(fields = Seq("*".f), from = Some(@@@("source")), where = @@("condition") is true)
          )))
    }

    it("should decompile create macro") {
      val model = CreateMacro(
        ref = DatabaseObjectRef("FETCH"),
        ifNotExists = false,
        `macro` = Macro(
          template = "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit",
          code = Select(fields = Seq("*".f), from = Some(@@@("source")), where = @@("condition") is true)
        ))
      assert(model.toSQL ==
        """create macro FETCH := "FETCH from %L:source ?where +?%c:condition ?limit +?%e:limit" select * from @@source where @condition is true""")
    }

  }

}
