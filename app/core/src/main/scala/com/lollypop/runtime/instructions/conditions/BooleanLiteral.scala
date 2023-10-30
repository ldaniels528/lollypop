package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Literal
import com.lollypop.runtime.Scope

/**
 * Represents a Boolean literal value
 * @param value the given value
 */
case class BooleanLiteral(value: Boolean) extends Literal with RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = value

}

