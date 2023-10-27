package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.plastics.RuntimeClass
import com.qwery.runtime.{QweryVM, Scope}

/**
 * SuperClassesOf() function - returns the super-classes extended by a class or instance
 * @param expression the expression representing a class or instance
 * @example {{{
 *  superClassesOf(classOf("java.util.ArrayList"))
 * }}}
 */
case class SuperClassesOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Class[_]] = {
    (Option(QweryVM.execute(scope, expression)._3) map {
      case _class: Class[_] => _class
      case value => value.getClass
    }).map(RuntimeClass.getSuperClasses(_).toArray).orNull
  }
}

object SuperClassesOf extends FunctionCallParserE1(
  name = "superClassesOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_OBJECT_ORIENTED,
  description =
    """|Returns the super-classes extended by a class or instance
       |""".stripMargin,
  example = "superClassesOf(classOf('java.util.ArrayList'))")
