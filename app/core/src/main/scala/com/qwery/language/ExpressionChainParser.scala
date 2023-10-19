package com.qwery.language

import com.qwery.language.models.Expression

/**
 * Represents an Expression Chain Parser
 */
trait ExpressionChainParser extends LanguageParser {

  def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Expression]

}
