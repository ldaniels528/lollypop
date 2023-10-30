package com.lollypop.language

import com.lollypop.language.models.{Condition, Expression}

/**
 * Represents a Condition-To-Expression Post Parser
 */
trait ConditionToExpressionPostParser extends LanguageParser {

  def parseConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Expression]

}