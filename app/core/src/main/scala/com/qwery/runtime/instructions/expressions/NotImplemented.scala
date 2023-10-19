package com.qwery.runtime.instructions.expressions

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.NotImplemented._symbol

case class NotImplemented() extends RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Any = this.dieNotImplemented()

  override def toSQL: String = _symbol
}

object NotImplemented extends ExpressionParser {
  private val _symbol = "???"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = _symbol,
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = _symbol,
    description = s"`${_symbol}` can be used for marking methods that remain to be implemented.",
    example =
      """|def blowUp() := ???
         |try
         |  blowUp()
         |catch e =>
         |  out <=== e.getMessage()
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf(_symbol)) Option(NotImplemented()) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}