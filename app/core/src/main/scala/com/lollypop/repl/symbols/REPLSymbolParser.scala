package com.lollypop.repl.symbols

import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}

/**
 * Represents a REPL Symbol Language Parser
 * @param symbol the symbol character (e.g. "~")
 * @param f      a symbol factory function
 * @tparam I the [[Expression]] type
 */
abstract class REPLSymbolParser[I <: Expression](val symbol: String, f: () => I)
  extends ExpressionParser {

  override def help: List[HelpDoc] = Nil

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[I] = {
    if (ts.nextIf(symbol)) Some(f()) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is symbol

}

