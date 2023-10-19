package com.qwery.language

import com.qwery.language.models.Instruction

/**
 * Represents a Directive Parser
 */
trait DirectiveParser extends LanguageParser {

  def parseDirective(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Instruction]

}
