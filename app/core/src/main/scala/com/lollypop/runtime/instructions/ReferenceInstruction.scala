package com.lollypop.runtime.instructions

import com.lollypop.language.models.Instruction
import com.lollypop.runtime.DatabaseObjectRef

trait ReferenceInstruction extends Instruction {

  def ref: DatabaseObjectRef

}
