package com.qwery.runtime.instructions

import com.qwery.language.models.SourceCodeInstruction
import com.qwery.runtime.Scope
import qwery.io.IOCost

/**
 * Represents an executable instruction
 */
trait RuntimeInstruction extends SourceCodeInstruction {

  def execute()(implicit scope: Scope): (Scope, IOCost, Any)

}
