package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.{Condition, Instruction}
import com.lollypop.runtime.{LollypopVM, Scope}

case class AssumeCondition(instruction: Instruction) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    val (scope0, _, result0) = LollypopVM.execute(scope, instruction)
    result0 match {
      case b: Boolean => b
      case o: Option[_] => o.nonEmpty
      case z => instruction.dieExpectedBoolean(z)
    }
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