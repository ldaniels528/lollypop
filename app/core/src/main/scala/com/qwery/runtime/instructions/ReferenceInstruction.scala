package com.qwery.runtime.instructions

import com.qwery.language.models.Instruction
import com.qwery.runtime.DatabaseObjectRef

trait ReferenceInstruction extends Instruction {

  def ref: DatabaseObjectRef

}
