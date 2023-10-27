package com.qwery.runtime.instructions.conditions

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, Scope}

import scala.annotation.tailrec

/**
 * SQL: `expression` is not null
 * @param expr the [[Expression expression]] to evaluate
 */
case class IsNotNull(expr: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    @tailrec
    def isntNull(value: Any): Boolean = value match {
      case Some(v) => isntNull(v)
      case null | None => false
      case _ => true
    }

    QweryVM.execute(scope, expr) ~> { case (_, _, result1) => isntNull(result1) }
  }

  override def toSQL: String = s"${expr.toSQL} is not null"
}

object IsNotNull extends FunctionCallParserE1(
  name = "isNotNull",
  category = CATEGORY_FILTER_MATCH_OPS,
  paradigm = PARADIGM_IMPERATIVE,
  description = "Returns true if the expression is not null, otherwise false.",
  example = "isNotNull('yes')")