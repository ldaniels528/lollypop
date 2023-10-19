package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.{Expression, Instruction}
import com.qwery.runtime.{QweryVM, Scope}

case class AssumeExpression(instruction: Instruction) extends RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    QweryVM.execute(scope, instruction)._3
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