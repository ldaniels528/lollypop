package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Instruction
import com.lollypop.runtime.datatypes.BooleanType
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

case class AssumeCondition(instruction: Instruction) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (s, c, r) = instruction.execute(scope)
    (s, c, BooleanType.convert(r))
  }

  override def toSQL: String = instruction.toSQL
}