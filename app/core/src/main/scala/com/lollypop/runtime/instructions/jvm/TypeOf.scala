package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.CATEGORY_JVM_REFLECTION
import com.lollypop.language.models.Expression
import com.lollypop.runtime.instructions.expressions.{RuntimeExpression, StringExpression}
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.StringRenderHelper
import lollypop.io.IOCost

/**
 * TypeOf() function - returns the JVM type of an expression
 * @param expression the expression being tested
 * @example {{{
 *  val b = "HELLO".getBytes()
 *  typeof(b) // byte[]
 * }}}
 */
case class TypeOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression with StringExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, String) = {
    val result = Option(LollypopVM.execute(scope, expression)._3).map(_.getClass).map(StringRenderHelper.toClassString).orNull
    (scope, IOCost.empty, result)
  }

}

object TypeOf extends FunctionCallParserE1(
  name = "typeOf",
  category = CATEGORY_JVM_REFLECTION,
  description =
    """|Returns the type of an expression.
       |""".stripMargin,
  example =
    """|counter = 5
       |typeOf(counter)
       |""".stripMargin)
