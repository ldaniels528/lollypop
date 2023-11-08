package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import lollypop.io.IOCost

/**
 * Returns true if the expression is null, otherwise false.
 * @param expr the [[Expression expression]] to evaluate
 */
case class IsNull(expr: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (s, c, r) = expr.execute(scope)
    (s, c, r == null)
  }

  override def toSQL: String = s"${expr.toSQL} is null"
}

object IsNull extends FunctionCallParserE1(
  name = "isNull",
  category = CATEGORY_FILTER_MATCH_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Returns true if the expression is null, otherwise false.",
  example = "isNull(null)")