package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Literal
import com.qwery.runtime.Scope

/**
 * Represents a Boolean literal value
 * @param value the given value
 */
case class BooleanLiteral(value: Boolean) extends Literal with RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = value

}

