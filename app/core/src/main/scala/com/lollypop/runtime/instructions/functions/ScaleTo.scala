package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.CATEGORY_TRANSFORMATION
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.{DoubleExpression, RuntimeExpression}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * Returns the the numeric expression rounded by scale decimal places
 * @param numberExpr the [[Expression value]] to scale
 * @param scaleExpr  the rounding [[Expression scale]]
 */
case class ScaleTo(numberExpr: Expression, scaleExpr: Expression) extends ScalarFunctionCall
  with RuntimeExpression with DoubleExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Double) = {
    val result = (for {
      number <- numberExpr.asDouble
      scale <- scaleExpr.asInt32
      factor = Math.pow(10, scale)
      output = (number * factor).longValue() / factor
    } yield output) || 0.0
    (scope, IOCost.empty, result)
  }

}

object ScaleTo extends FunctionCallParserE2(
  name = "scaleTo",
  category = CATEGORY_TRANSFORMATION,
  description = "Returns the the numeric expression truncated after `scale` decimal places.",
  example = "scaleTo(0.567, 2)")
