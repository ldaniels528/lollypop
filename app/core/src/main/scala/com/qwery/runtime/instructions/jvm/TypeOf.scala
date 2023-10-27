package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.CATEGORY_JVM_REFLECTION
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.{RuntimeExpression, StringExpression}
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.StringRenderHelper

/**
 * TypeOf() function - returns the JVM type of an expression
 * @param expression the expression being tested
 * @example {{{
 *  val b = "HELLO".getBytes()
 *  typeof(b) // byte[]
 * }}}
 */
case class TypeOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression with StringExpression {

  override def evaluate()(implicit scope: Scope): String = {
    Option(QweryVM.execute(scope, expression)._3).map(_.getClass).map(StringRenderHelper.toClassString).orNull
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
