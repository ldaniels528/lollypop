package com.lollypop.language

import com.lollypop.language.models.Instruction

/**
 * Represents an Instruction Postfix Parser
 */
trait InstructionPostfixParser extends LanguageParser {

  def parseInstructionChain(ts: TokenStream, host: Instruction)(implicit compiler: SQLCompiler): Option[Instruction]

}
