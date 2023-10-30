package com.lollypop.runtime.instructions

import com.lollypop.language.models.SourceCodeInstruction
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * Represents an executable instruction
 */
trait RuntimeInstruction extends SourceCodeInstruction {

  def execute()(implicit scope: Scope): (Scope, IOCost, Any)

}
