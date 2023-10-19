package com.qwery.runtime.instructions.jvm

import com.qwery.language.HelpDoc.{CATEGORY_REFLECTION, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Expression
import com.qwery.runtime.datatypes.Inferences
import com.qwery.runtime.instructions.expressions.{RuntimeExpression, StringExpression}
import com.qwery.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.qwery.runtime.{QweryVM, Scope}

/**
 * CodecOf() function - returns the CODEC (encoder/decoder) of an expression
 * @param expression the expression being tested
 * @example {{{
 *  val b = "HELLO".getBytes()
 *  codecOf(b) // VarBinary(5)
 * }}}
 */
case class CodecOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression with StringExpression {
  override def evaluate()(implicit scope: Scope): String = {
    val value = QweryVM.execute(scope, expression)._3
    Inferences.fromValue(value).toSQL
  }
}

object CodecOf extends FunctionCallParserE1(
  name = "codecOf",
  category = CATEGORY_REFLECTION,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Returns the CODEC (encoder/decoder) of an expression.
       |""".stripMargin,
  example =
    """|val counter = 5
       |codecOf(counter)
       |""".stripMargin)
