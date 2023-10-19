package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.CATEGORY_TESTING
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Inequality.toInequalities
import com.qwery.language.models.{Condition, Expression}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.util.OptionHelper.OptionEnrichment

import scala.util.Try

/**
 * Represents a Kung Fu Verification
 * @param condition the [[Condition condition]] to test
 * @param message     the optional title [[Expression message]]
 */
case class Verify(condition: Condition, message: Option[Expression] = None) extends Verification {
  val title: Option[Expression] = message ?? alias.map(_.v)

  override def determineMismatches(scope: Scope): List[String] = {
    val inequalities = toInequalities(condition)
    inequalities.collect { case inEq if !Try(RuntimeCondition.isTrue(inEq)(scope)).toOption.contains(true) => inEq.negate.toSQL }
  }

  override def isTrue(implicit scope: Scope): Boolean = RuntimeCondition.isTrue(condition)

  override def toSQL: String = ("verify" :: condition.toSQL :: title.toList.flatMap(t => List("^^^", t.toSQL))).mkString(" ")

}

/**
 * Verify Parser
 */
object Verify extends ExpressionParser {
  private val template = "verify %c:condition ?^^^ +?%e:title"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "verify",
    category = CATEGORY_TESTING,
    syntax = template,
    description = "Verifies the current state of the scope",
    example =
      """|response = { id: 357 }
         |verify response.id is 357
         |          ^^^ 'Success!'
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Verify] = {
    val params = SQLTemplateParams(ts, template)
    Some(Verify(condition = params.conditions("condition"), message = params.expressions.get("title")))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "verify"

}
