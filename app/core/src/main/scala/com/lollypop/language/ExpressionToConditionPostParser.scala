package com.lollypop.language

import com.lollypop.language.models.{Condition, Expression}

/**
 * Represents an Expression-To-Condition Post Parser
 */
trait ExpressionToConditionPostParser extends LanguageParser {

  def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition]

}