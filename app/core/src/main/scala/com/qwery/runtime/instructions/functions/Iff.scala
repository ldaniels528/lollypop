package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Condition, Expression}
import com.qwery.runtime.instructions.conditions.RuntimeCondition.isTrue
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.{QweryVM, Scope}

/**
 * If the condition evaluates to true, then returns `trueValue`; otherwise returns `falseValue`.
 * @param condition  the [[Condition condition]] to evaluate
 * @param trueValue  is returned if the condition evaluates to `true`
 * @param falseValue is returned if the condition evaluates to `false`
 * @example iff(value > 5, 'Yes', 'No')
 */
case class Iff(condition: Expression, trueValue: Expression, falseValue: Expression)
  extends ScalarFunctionCall with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    QweryVM.execute(scope, if (isTrue(condition)) trueValue else falseValue)._3
  }

}

object Iff extends FunctionCallParserE3(
  name = "iff",
  category = CATEGORY_CONTROL_FLOW,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "If the `condition` is true, then `trueValue` otherwise `falseValue`",
  example =
    """|value = 123
       |iff(value > 99, 'Yes!', 'No.')
       |""".stripMargin
)