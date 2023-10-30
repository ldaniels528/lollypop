package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_IMPERATIVE}
import com.lollypop.language.models.{Expression, IdentifierRef}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.expressions.NamedFunctionCall
import com.lollypop.runtime.instructions.functions.{AnonymousNamedFunction, FunctionCallParserE1, ScalarFunctionCall}

case class IsDefined(expression: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val name_? = expression match {
      case AnonymousNamedFunction(name) => Some(name)
      case NamedFunctionCall(name, _) => Some(name)
      case i: IdentifierRef => Some(i.name)
      case _ => None
    }
    name_?.exists(scope.isDefined)
  }

}

object IsDefined extends FunctionCallParserE1(
  name = "isDefined",
  category = CATEGORY_FILTER_MATCH_OPS,
  paradigm = PARADIGM_IMPERATIVE,
  description = "Returns true if the field or variable exists within the scope.",
  example = "isDefined(counter)")
