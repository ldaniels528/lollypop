package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.VariableRef
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * Represents a reference to a scalar variable
 * @param name the name of the scalar variable
 */
case class ScalarVariableRef(name: String) extends RuntimeExpression with VariableRef {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, scope.getValueReferences.getOrElse(name, dieNoSuchVariable(name)).value)
  }

  override def toSQL: String = s"@$name"

}
