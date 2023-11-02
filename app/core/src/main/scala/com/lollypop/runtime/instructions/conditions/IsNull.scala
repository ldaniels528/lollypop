package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * SQL: `expression` is null
 * @param expr the [[Expression expression]] to evaluate
 */
case class IsNull(expr: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost.empty, LollypopVM.execute(scope, expr)._3 == null)
  }

  override def toSQL: String = s"${expr.toSQL} is null"
}

object IsNull extends FunctionCallParserE1(
  name = "isNull",
  category = CATEGORY_FILTER_MATCH_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description = "Returns true if the expression is null, otherwise false.",
  example = "isNull(null)")