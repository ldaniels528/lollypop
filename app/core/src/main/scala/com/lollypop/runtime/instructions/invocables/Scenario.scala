package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_TESTING, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{CodeBlock, Expression, Instruction, Literal}
import com.lollypop.runtime.instructions.conditions.Verification
import com.lollypop.runtime.instructions.expressions.WWW
import com.lollypop.runtime.instructions.invocables.Scenario.{__AUTO_EXPAND__, __KUNGFU_BASE_URL__}
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Scenario declaration
 * @param title         the scenario title
 * @param verifications the scenario [[Instruction verifications]]
 * @param inherits      the optional inherited scopes referenced by scenario title
 * @example {{{
 *   feature "State Inheritance" {
 *     scenario 'Create a contest' {
 *       val contest_id = "40d1857b-474c-4400-8f07-5e04cbacc021"
 *       var counter = 1
 *       stderr <=== "contest_id = {{contest_id}}, counter = {{counter}}\n"
 *       verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
 *          and counter is 1
 *     }
 *   }
 * }}}
 */
case class Scenario(title: Expression,
                    verifications: Seq[Instruction],
                    inherits: Option[Expression] = None) extends RuntimeInvokable {

  override def execute()(implicit scope0: Scope): (Scope, IOCost, Any) = {
    val accum = verifications.foldLeft[Accumulator](Accumulator(scope0)) {
      case (acc, verification: Verification) => validate(acc, verification)
      case (acc, instruction) => run(acc, instruction)
    }
    (accum.scope, accum.cost, accum.outcome)
  }

  private def validate(acc: Accumulator, verify: Verification): Accumulator = {
    try {
      val (scope1, cost1, result) = verify.execute(acc.scope)
      val isPassed = result == true
      acc.copy(scope = scope1, outcome = acc.outcome && isPassed, cost = acc.cost ++ cost1)
    } catch {
      case e: Exception =>
        acc.copy(outcome = false)
    }
  }

  private def run(acc: Accumulator, instruction: Instruction): Accumulator = {
    try {
      val (scope1, cost1, result1) = instruction.execute(acc.scope)
      val scope2 = captureVariables(scope1, instruction)
      result1 match {
        case p: Product if scope1.resolve(__AUTO_EXPAND__).contains(true) =>
          val kvps = p.productElementNames.toSeq zip p.productIterator
          val scope3 = kvps.foldLeft[Scope](scope2) {
            case (agg, (name, value)) => agg.withVariable(name, value)
          }
          acc.copy(scope = scope3.withVariable("result", p), cost = acc.cost ++ cost1, outcome = true)
        case x =>
          acc.copy(scope = scope2.withVariable("result", x), cost = acc.cost ++ cost1, outcome = true)
      }
    }
    catch {
      case e: Exception =>
        acc.copy(outcome = false)
    }
  }

  private def captureVariables(scope: Scope, instruction: Instruction): Scope = {
    def getBaseURL(url: String): String = url.lastIndexOf('/') match {
      case -1 => url
      case n => url.substring(0, n)
    }

    instruction match {
      case WWW(_, Literal(url: String), _, _) =>
        scope.withVariable(__KUNGFU_BASE_URL__, code = getBaseURL(url).v, isReadOnly = false)
      case _ => scope
    }
  }

  override def toSQL: String = {
    (s"scenario ${title.toSQL}${inherits.map(e => s" extends ${e.toSQL}") || ""} {" ::
      verifications.toList.map(v => s"\t${v.toSQL}") ::: "}" :: Nil).mkString("\n")
  }

  case class Accumulator(scope: Scope, cost: IOCost = IOCost.empty, outcome: Boolean = true)

}

/**
 * Scenario Parser
 */
object Scenario extends InvokableParser {
  val __AUTO_EXPAND__ = "__AUTO_EXPAND__"
  val __KUNGFU_BASE_URL__ = "__KUNGFU_BASE_URL__"
  val template = "scenario %e:title ?extends +?%e:inheritance %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "scenario",
    category = CATEGORY_TESTING,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "scenario-based test declaration",
    example =
      """|feature "State Inheritance" {
         |  scenario 'Create a contest' {
         |    val contest_id = "40d1857b-474c-4400-8f07-5e04cbacc021"
         |    var counter = 1
         |    stderr <=== "contest_id = {{contest_id}}, counter = {{counter}}\n"
         |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
         |        and counter is 1
         |  }
         |
         |  scenario 'Create a member' {
         |    val member_id = "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
         |    stderr <=== "member_id = {{member_id}}\n"
         |    verify member_id is "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
         |  }
         |
         |  scenario 'Inherit contest state' extends 'Create a contest' {
         |    counter = counter + 1
         |    stderr <=== "contest_id = {{contest_id}}, counter = {{counter}}\n"
         |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
         |        and counter is 2
         |  }
         |
         |  scenario 'Inherit contest and member state' extends ['Create a contest', 'Create a member'] {
         |    counter = counter + 1
         |    stderr <=== "contest_id = {{contest_id}}, member_id = {{member_id}}, counter = {{counter}}\n"
         |    verify contest_id is "40d1857b-474c-4400-8f07-5e04cbacc021"
         |        and member_id is "4264f8a5-6fa3-4a38-b3bb-30e2e0b826d1"
         |        and counter is 3
         |  }
         |}
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Scenario] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(Scenario(
        title = params.expressions("title"),
        verifications = params.extractCode,
        inherits = params.expressions.get("inheritance")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "scenario"
}
