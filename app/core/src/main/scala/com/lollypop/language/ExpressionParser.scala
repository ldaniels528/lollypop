package com.lollypop.language

import com.lollypop.language.models.Expression

/**
 * Represents an Expression Parser
 */
trait ExpressionParser extends LanguageParser {

  def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression]

}