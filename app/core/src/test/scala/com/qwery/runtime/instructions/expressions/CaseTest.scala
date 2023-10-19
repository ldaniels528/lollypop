package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.language.models.{@@, Expression}
import com.qwery.language.{SQLTemplateParams, Template}
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.conditions.{Like, Not}
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CaseTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Case].getSimpleName) {

    it("should use the command's template to parse a statement") {
      val params = Template(Case.templateExpression).processWithDebug(
        """|case @sector
           |  when 'Oil & Gas Production' then 'Oil-Gas'
           |  when 'Public Utilities' then 'Pub Utils'
           |  else 'Unknown'
           |end
           |""".stripMargin)
      assert(params.all == Map(
        "caseExpr" -> @@("sector"),
        "elseExpr" -> ("Unknown": Expression),
        "keywords" -> Set("case", "else", "end"),
        "1" -> List(SQLTemplateParams(
          keywords = Set("when"),
          atoms = Map("t" -> "then"),
          instructions = Map("whenExpr" -> "Oil & Gas Production".v, "thenExpr" -> "Oil-Gas".v)
        )),
        "2" -> List(SQLTemplateParams(
          keywords = Set("when"),
          atoms = Map("t" -> "then"),
          instructions = Map("whenExpr" -> "Public Utilities".v, "thenExpr" -> "Pub Utils".v)
        ))
      ))
    }

    it("should evaluate (Type 1)") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|set sector = "Public Utilities"
           |select case sector
           |  when 'Oil & Gas Production' -> 'Oil-Gas'
           |  when 'Public Utilities' -> 'Pub Utils'
           |  else 'Unknown'
           |end as sector
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(Map("sector" -> "Pub Utils")))
    }

    it("should evaluate (Type 2)") {
      val scope = Scope()
        .withVariable("sector", "Public Utilities")
      val (_, _, device) = QweryVM.searchSQL(scope,
        """|select case
           |  when @sector == 'Oil & Gas Production' then 'Oil-Gas'
           |  when @sector == 'Public Utilities' then 'Pub Utils'
           |  else 'Unknown'
           |end as sector
           |""".stripMargin)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(Map("sector" -> "Pub Utils")))
    }

    it("should evaluate (Type 3)") {
      val scope = Scope()
        .withVariable("value", 88.77)
      val (_, _, result) = QweryVM.executeSQL(scope,
        """|case
           |  when value.?intValue() -> value.intValue()
           |  else null
           |end
           |""".stripMargin)
      assert(result contains 88)
    }

    it("should parse: case field ...") {
      assert {
        compiler.compile(
          """|select case Sector
             |  when 'Oil & Gas Production' then 'Oil-Gas'
             |  when 'Public Utilities' then 'Pub Utils'
             |  else 'Unknown'
             |end
             |""".stripMargin) == Select(fields = Seq(
          Case(otherwise = "Unknown": Expression, conditions = List(
            Case.When("Sector".f === "Oil & Gas Production", "Oil-Gas"),
            Case.When("Sector".f === "Public Utilities", "Pub Utils"))
          )))
      }
    }

    it("should parse: case when field ...") {
      verify(
        """|case
           |  when Sector == 'Oil & Gas Production' then 'Oil-Gas'
           |  when Sector == 'Public Utilities' then 'Pub Utils'
           |  else 'Unknown'
           |end
           |""".stripMargin,
        Case(otherwise = "Unknown": Expression, conditions = List(
          Case.When("Sector".f === "Oil & Gas Production", "Oil-Gas"),
          Case.When("Sector".f === "Public Utilities", "Pub Utils")),
        ))
    }

    it("should decompile: case when field ...") {
      verify(
        """|select case @sector
           |  when 'Oil & Gas Production' then 'Oil-Gas'
           |  when 'Public Utilities' then 'Pub Utils'
           |  else 'Unknown'
           |end
           |""".stripMargin)
    }

    it("""should parse "case when field not like '%.%' then 'yes' else 'No' end" """) {
      verify(
        """|case
           |  when not field like '%.%' then 'Yes' else 'No'
           |end
           |""".stripMargin,
        Case(conditions = List(Case.When(Not(Like("field".f, "%.%")), "Yes": Expression)), otherwise = Some("No": Expression)))
    }

  }

}
