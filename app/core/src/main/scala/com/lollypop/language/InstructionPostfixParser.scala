package com.lollypop.language

import com.lollypop.language.models.Instruction

/**
 * Represents an Instruction Postfix Parser
 */
trait InstructionPostfixParser[T <: Instruction] extends LanguageParser {

  def parseInstructionChain(ts: TokenStream, host: T)(implicit compiler: SQLCompiler): Option[T]

}
