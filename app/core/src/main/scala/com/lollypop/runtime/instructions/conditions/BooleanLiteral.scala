package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Literal
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * Represents a Boolean literal value
 * @param value the given value
 */
case class BooleanLiteral(value: Boolean) extends Literal with RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost.empty, value)
  }
}

