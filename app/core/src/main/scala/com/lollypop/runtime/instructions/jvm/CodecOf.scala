package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.Inferences
import com.lollypop.runtime.instructions.expressions.{RuntimeExpression, StringExpression}
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import lollypop.io.IOCost

/**
 * CodecOf() function - returns the CODEC (encoder/decoder) of an expression
 * @param expression the expression being tested
 * @example {{{
 *  val b = "HELLO".getBytes()
 *  codecOf(b) // VarBinary(5)
 * }}}
 */
case class CodecOf(expression: Expression) extends ScalarFunctionCall with RuntimeExpression with StringExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, String) = {
    val value = expression.execute(scope)._3
    (scope, IOCost.empty, Inferences.fromValue(value).toSQL)
  }
}

object CodecOf extends FunctionCallParserE1(
  name = "codecOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Returns the CODEC (encoder/decoder) of an expression.
       |""".stripMargin,
  example =
    """|val counter = 5
       |codecOf(counter)
       |""".stripMargin)
