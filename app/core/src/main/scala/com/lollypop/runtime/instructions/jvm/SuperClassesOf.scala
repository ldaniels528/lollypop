package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.plastics.RuntimeClass
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * SuperClassesOf() function - returns the super-classes extended by a class or instance
 * @param expression the expression representing a class or instance
 * @example {{{
 *  superClassesOf(classOf("java.util.ArrayList"))
 * }}}
 */
case class SuperClassesOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Class[_]] = {
    (Option(LollypopVM.execute(scope, expression)._3) map {
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
