package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Instruction
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

case class AssumeExpression(instruction: Instruction) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    instruction.execute(scope)
  }

  override def toSQL: String = instruction.toSQL

}