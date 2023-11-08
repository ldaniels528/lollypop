package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

case class AssumeExpression(instruction: Instruction) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    instruction.execute(scope)
  }

  override def toSQL: String = instruction.toSQL

}

object AssumeExpression {
  final implicit class EnrichedAssumeExpression(val instruction: Instruction) extends AnyVal {
    @inline
    def asExpression: Expression = {
      instruction match {
        case expression: Expression => expression
        case other => new AssumeExpression(other)
      }
    }
  }
}