package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.CATEGORY_SCIENCE
import com.qwery.language.models.Expression
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.expressions.{DoubleExpression, RuntimeExpression}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Returns the the numeric expression rounded by scale decimal places
 * @param numberExpr the [[Expression value]] to scale
 * @param scaleExpr  the rounding [[Expression scale]]
 */
case class ScaleTo(numberExpr: Expression, scaleExpr: Expression) extends ScalarFunctionCall
  with RuntimeExpression with DoubleExpression {
  override def evaluate()(implicit scope: Scope): Double = {
    (for {
      number <- numberExpr.asDouble
      scale <- scaleExpr.asInt32
      factor = Math.pow(10, scale)
      output = (number * factor).longValue() / factor
    } yield output) || 0.0
  }

}

object ScaleTo extends FunctionCallParserE2(
  name = "scaleTo",
  category = CATEGORY_SCIENCE,
  description = "Returns the the numeric expression truncated after `scale` decimal places.",
  example = "scaleTo(0.567, 2)")
