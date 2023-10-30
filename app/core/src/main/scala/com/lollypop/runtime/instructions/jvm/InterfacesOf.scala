package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * InterfacesOf() function - returns the interfaces implemented by a class or instance
 * @param expression the expression representing a class or instance
 * @example {{{
 *  interfacesOf(classOf("java.util.ArrayList"))
 * }}}
 */
case class InterfacesOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Class[_]] = {
    (Option(LollypopVM.execute(scope, expression)._3) map {
      case _class: Class[_] => _class
      case value => value.getClass
    }).map(RuntimeClass.getInterfaces(_).toArray).orNull
  }
}

object InterfacesOf extends FunctionCallParserE1(
  name = "interfacesOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description =
    """|Returns the interfaces implemented by a class or instance
       |""".stripMargin,
  example = "interfacesOf(classOf('java.util.ArrayList'))")