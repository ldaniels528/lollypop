package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_OBJECT_ORIENTED}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.plastics.RuntimeClass
import lollypop.io.IOCost

/**
 * SuperClassesOf() function - returns the super-classes extended by a class or instance
 * @param expression the expression representing a class or instance
 * @example {{{
 *  superClassesOf(classOf("java.util.ArrayList"))
 * }}}
 */
case class SuperClassesOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[Class[_]]) = {
    val result = (Option(expression.execute(scope)._3) map {
      case _class: Class[_] => _class
      case value => value.getClass
    }).map(RuntimeClass.getSuperClasses(_).toArray).orNull
    (scope, IOCost.empty, result)
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
