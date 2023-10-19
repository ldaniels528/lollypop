package com.qwery.language

import com.qwery.language.models.Condition

/**
 * Represents a Condition-To-Condition Post Parser
 */
trait ConditionToConditionPostParser extends LanguageParser {

  def parseConditionChain(ts: TokenStream, host: Condition)(implicit compiler: SQLCompiler): Option[Condition]

}