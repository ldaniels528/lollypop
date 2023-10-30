package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.NotImplemented._symbol

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
         |  stdout <=== e.getMessage()
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (ts.nextIf(_symbol)) Option(NotImplemented()) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}