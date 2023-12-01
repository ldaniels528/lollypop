package com.lollypop.repl.symbols

import com.lollypop.language.models.{BinaryOperation, Expression}
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.repl.symbols.ColonSlashSlash.keyword

/**
 * Colon-Slash-Slash symbol (://)
 * @example {{{
 *  www https://0.0.0.0/api?symbol=ABC
 * }}}
 */
case class ColonSlashSlash(a: Expression, b: Expression) extends BinaryOperation {
  override def operator: String = keyword
}

object ColonSlashSlash extends ExpressionChainParser {
  private val keyword = "://"

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression] = {
    stream match {
      case ts if ts nextIf keyword => for (b <- compiler.nextExpression(ts)) yield ColonSlashSlash(host, b)
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
