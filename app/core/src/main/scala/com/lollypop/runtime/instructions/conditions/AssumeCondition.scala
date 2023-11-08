package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.{Condition, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.datatypes.BooleanType
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

case class AssumeCondition(instruction: Instruction) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (s, c, r) = instruction.execute(scope)
    (s, c, BooleanType.convert(r))
  }

  override def toSQL: String = instruction.toSQL
}

object AssumeCondition {
  final implicit class EnrichedAssumeCondition(val instruction: Instruction) extends AnyVal {
    @inline
    def asCondition: Condition = {
      instruction match {
        case condition: Condition => condition
        case other => new AssumeCondition(other)
      }
    }
  }
}