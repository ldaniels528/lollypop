package com.lollypop.language

import com.lollypop.language.models.Expression

/**
 * Represents an Expression Chain Parser
 */
trait ExpressionChainParser extends LanguageParser {

  def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression]

}
