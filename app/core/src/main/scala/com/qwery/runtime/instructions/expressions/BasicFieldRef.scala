package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.FieldRef
import com.qwery.runtime.QweryVM.implicits.RichScalaAny
import com.qwery.runtime.Scope

/**
 * Represents a generic field
 * @param name the name of the [[FieldRef field reference]]
 */
case class BasicFieldRef(name: String) extends FieldRef with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = scope(name).unwrapOptions

  override def toSQL: String = if (name.forall(c => c.isLetterOrDigit || c == '_')) name else s"`$name`"

}