package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.plastics.RuntimeClass.getClassByName

/**
 * ClassOf() function - returns the Class instance for a given classname
 * @param expression the fully qualified classname (e.g. "java.util.Date")
 * @example {{{
 *  classOf("java.util.Date") // `java.util.Date`
 * }}}
 */
case class ClassOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Class[_] = {
    expression.asString.map(getClassByName).orNull
  }
}

object ClassOf extends FunctionCallParserE1(
  name = "classOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description = "Returns a class instance by name (e.g. \"Class.forName\")",
  example = "classOf('java.io.File')"
)
