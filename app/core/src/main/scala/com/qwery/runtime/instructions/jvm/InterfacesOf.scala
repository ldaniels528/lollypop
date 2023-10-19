package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, RuntimeClass, Scope}

/**
 * InterfacesOf() function - returns the interfaces implemented by a class or instance
 * @param expression the expression representing a class or instance
 * @example {{{
 *  interfacesOf(classOf("java.util.ArrayList"))
 * }}}
 */
case class InterfacesOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Class[_]] = {
    (Option(QweryVM.execute(scope, expression)._3) map {
      case _class: Class[_] => _class
      case value => value.getClass
    }).map(RuntimeClass.getInterfaces(_).toArray).orNull
  }
}

object InterfacesOf extends FunctionCallParserE1(
  name = "interfacesOf",
  category = CATEGORY_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description =
    """|Returns the interfaces implemented by a class or instance
       |""".stripMargin,
  example = "interfacesOf(classOf('java.util.ArrayList'))")
