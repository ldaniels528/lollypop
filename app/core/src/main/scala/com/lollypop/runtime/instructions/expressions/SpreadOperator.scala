package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_JVM_REFLECTION, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}

/**
 * Spread Operator (e.g., "...")
 * @see [[com.lollypop.runtime.instructions.functions.FunctionArguments]]
 */
case class SpreadOperator(host: Expression) extends Expression {
  override def toSQL: String = host.toSQL + "..."
}

object SpreadOperator extends ExpressionChainParser {
  private val _symbol = "..."

  override def help: List[HelpDoc] = List(HelpDoc(
    name = _symbol,
    category = CATEGORY_JVM_REFLECTION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = _symbol,
    description = "The argument spread operator: can convert an array into individual arguments",
    example =
      """|def p3d(x: Double, y: Double, z: Double) := (x, y, z)
         |
         |p3d([ x: 123, y:13, z: 67 ]...)
         |""".stripMargin
  ), HelpDoc(
    name = _symbol,
    category = CATEGORY_JVM_REFLECTION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = _symbol,
    description = "The argument spread operator: can convert a dictionary into individual arguments",
    example =
      """|def p3d(x: Double, y: Double, z: Double) := (x, y, z)
         |
         |p3d({ x: 123, y:13, z: 67 }...)
         |""".stripMargin
  ))

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf(_symbol)) Option(SpreadOperator(host)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}