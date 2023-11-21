package com.lollypop.runtime.instructions.operators

import com.lollypop.language.models.{Expression, UnaryOperation}
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

case class Tilde(a: Expression) extends RuntimeExpression with UnaryOperation {
  override val operator: String = "~"

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, this)
  }

}

object Tilde extends ExpressionParser {

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Tilde] = {
    if (ts nextIf "~") compiler.nextExpression(ts).map(Tilde.apply) ?? ts.dieExpectedNumeric() else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "~"

}