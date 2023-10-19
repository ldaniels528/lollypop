package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.VariableRef
import com.qwery.runtime.Scope

/**
 * Represents a reference to a scalar variable
 * @param name the name of the scalar variable
 */
case class ScalarVariableRef(name: String) extends RuntimeExpression with VariableRef {

  override def evaluate()(implicit scope: Scope): Any = {
    scope.getValueReferences.getOrElse(name, dieNoSuchVariable(name)).value
  }

  override def toSQL: String = s"@$name"

}
