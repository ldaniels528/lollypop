package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.plastics.RuntimeClass.getClassByName
import lollypop.io.IOCost

/**
 * ClassOf() function - returns the Class instance for a given classname
 * @param expression the fully qualified classname (e.g. "java.util.Date")
 * @example {{{
 *  classOf("java.util.Date") // `java.util.Date`
 * }}}
 */
case class ClassOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Class[_]) = {
    (scope, IOCost.empty, expression.asString.map(getClassByName).orNull)
  }
}

object ClassOf extends FunctionCallParserE1(
  name = "classOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description = "Returns a class instance by name (e.g. \"Class.forName\")",
  example = "classOf('java.io.File')"
)
