package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.FieldRef
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a generic field
 * @param name the name of the [[FieldRef field reference]]
 */
case class BasicFieldRef(name: String) extends FieldRef with RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, scope(name).unwrapOptions)
  }

  override def toSQL: String = if (name.forall(c => c.isLetterOrDigit || c == '_')) name else s"`$name`"

}