package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_IMPERATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.{RuntimeExpression, StringExpression}
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
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
    expression.execute(scope) ~>> (v => Option(v).map(_.getClass).map(StringRenderHelper.toClassString).orNull)
  }

}

object TypeOf extends FunctionCallParserE1(
  name = "typeOf",
  category = CATEGORY_JVM_REFLECTION,
  paradigm = PARADIGM_IMPERATIVE,
  description =
    """|Returns the type of an expression.
       |""".stripMargin,
  example =
    """|counter = 5
       |typeOf(counter)
       |""".stripMargin)
