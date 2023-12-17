package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.CATEGORY_TRANSFORMATION
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.expressions.{DoubleExpression, RuntimeExpression}
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Returns the the numeric expression rounded by scale decimal places
 * @param numberExpr the [[Expression value]] to scale
 * @param scaleExpr  the rounding [[Expression scale]]
 */
case class ScaleTo(numberExpr: Expression, scaleExpr: Expression) extends ScalarFunctionCall
  with RuntimeExpression with DoubleExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Double) = {
    val (sa, ca, number) = numberExpr.pullDouble
    val (sb, cb, scale) = scaleExpr.pullInt(sa)
    val factor = Math.pow(10, scale)
    val result = (number * factor).longValue() / factor
    (sb, ca ++ cb, result)
  }

}

object ScaleTo extends FunctionCallParserE2(
  name = "scaleTo",
  category = CATEGORY_TRANSFORMATION,
  description = "Returns the the numeric expression truncated after `scale` decimal places.",
  example = "scaleTo(0.567, 2)")
