package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_TESTING, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.language.models.Inequality._
import com.qwery.runtime.instructions.conditions.AssumeCondition.EnrichedAssumeCondition
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE2, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Represents a Assertion
 * @param condition the [[Expression condition]] to test
 * @param message   the message to display if the condition is not satisfied
 */
case class Assert(condition: Expression, message: Expression) extends ScalarFunctionCall with Verification {
  private val _title = condition.alias.map(_.v)

  override def determineMismatches(scope: Scope): List[String] = {
    val inequalities = toInequalities(condition.asCondition)
    inequalities.collect { case inEq if QweryVM.execute(scope, inEq)._3 == false => inEq.negate.toSQL }
  }

  override def isTrue(implicit scope: Scope): Boolean = {
    if (RuntimeCondition.isTrue(condition)) true else die(message.asString || "Assertion failed")
  }

  override def title: Option[Expression] = _title

}

/**
 * Assert Parser
 */
object Assert extends FunctionCallParserE2(
  name = "assert",
  category = CATEGORY_TESTING,
  paradigm = PARADIGM_IMPERATIVE,
  description = "Assertion: if the expression evaluates to false, an exception is thrown.",
  examples = List(
    """|total = 99
       |assert(total < 100, 'total must be less than 100')
       |""".stripMargin,
    """|total = 101
       |try
       |  assert(total < 100, 'total must be less than 100')
       |catch e =>
       |  stderr <=== e.getMessage()
       |""".stripMargin
  )

)