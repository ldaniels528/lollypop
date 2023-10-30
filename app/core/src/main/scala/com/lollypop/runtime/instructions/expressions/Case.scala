package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.{Condition, Expression, Instruction}
import com.lollypop.runtime.instructions.conditions.AssumeCondition.EnrichedAssumeCondition
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment

/**
 * Represents a case expression
 * @example
 * {{{
 * case primary-expr
 *   when expr1 then result1
 *   when expr2 then result2
 *   else expr3
 * end
 * }}}
 * @example
 * {{{
 * case
 *   when value1 == expr1 then result1
 *   when value2 is expr2 then result2
 *   else expr3
 * end
 * }}}
 * @param conditions the list of when conditions
 */
case class Case(conditions: List[Case.When], otherwise: Option[Instruction]) extends RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Any = {
    ((conditions collectFirst {
      case Case.When(cond, result) if RuntimeCondition.isTrue(cond) => LollypopVM.execute(scope, result)._3
    }) ?? otherwise.map(LollypopVM.execute(scope, _)._3)).orNull
  }

  override def toSQL: String = {
    ("case\n" :: conditions.map(_.toSQL + "\n") ::: otherwise.toList.flatMap(i => List("else", i.toSQL)) ::: "\nend" :: Nil).mkString(" ")
  }
}

/**
 * Case Companion
 * @author lawrence.daniels@gmail.com
 */
object Case extends ExpressionParser {
  val templateExpression: String =
    """|case ?%e:caseExpr %OO {{
       |?when +?%e:whenExpr +?%C(t|then|->) +?%i:thenExpr
       |}}
       |?else +?%i:elseExpr
       |end
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "case",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateExpression,
    description = "An if-else alternative",
    example =
      """|val sector = 'Oil & Gas Production'
         |case sector
         |  when 'Financial Services' -> "Fin-Svc"
         |  when 'Oil & Gas Production' -> 'Oil-Gas'
         |  when 'Public Utilities' -> 'Pub-Utils'
         |  else 'Unknown'
         |end
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Case] = {
    val params = SQLTemplateParams(ts, templateExpression)
    val cases: List[Case.When] = params.expressions.get("caseExpr") match {
      case Some(caseExpr) =>
        params.repeatedSets.toList.sortBy(_._1.toInt).flatMap { case (_, listOfParams) =>
          listOfParams.map { params =>
            Case.When(caseExpr === params.expressions("whenExpr"), params.instructions("thenExpr"))
          }
        }
      case None =>
        params.repeatedSets.toList.sortBy(_._1.toInt).flatMap { case (_, listOfParams) =>
          listOfParams.map { params =>
            Case.When(params.expressions("whenExpr").asCondition, params.instructions("thenExpr"))
          }
        }
    }
    Option(Case(conditions = cases, otherwise = params.instructions.get("elseExpr")))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "case"

  /**
   * Represents a when condition
   * @param condition the given [[Condition condition]]
   * @param result    the given [[Expression result expression]]
   */
  case class When(condition: Condition, result: Instruction) extends Instruction {
    override def toSQL: String = s"when ${condition.toSQL} then ${result.toSQL}"
  }

}
