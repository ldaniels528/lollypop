package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.FieldRef
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a join field
 * @param tableAlias the given table alias
 * @param name       the name of the [[FieldRef field reference]]
 */
case class JoinFieldRef(tableAlias: String, name: String) extends FieldRef with RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, scope.getDataSourceValue(tableAlias, name).unwrapOptions)
  }

  override def toSQL: String = s"$tableAlias.$name"

}
