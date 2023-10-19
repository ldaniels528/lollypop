package com.qwery.language

import com.qwery.language.models.{Condition, Expression}

/**
 * Represents an Expression-To-Condition Post Parser
 */
trait ExpressionToConditionPostParser extends LanguageParser {

  def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition]

}