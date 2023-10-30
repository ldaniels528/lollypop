package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.{LollypopVM, Scope}

case class AssumeExpression(instruction: Instruction) extends RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    LollypopVM.execute(scope, instruction)._3
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