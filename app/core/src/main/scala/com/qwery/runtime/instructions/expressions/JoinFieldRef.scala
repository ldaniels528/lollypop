package com.qwery.runtime.instructions.expressions

import com.qwery.language.models.FieldRef
import com.qwery.runtime.QweryVM.implicits.RichScalaAny
import com.qwery.runtime.Scope

/**
 * Represents a join field
 * @param tableAlias the given table alias
 * @param name       the name of the [[FieldRef field reference]]
 */
case class JoinFieldRef(tableAlias: String, name: String) extends FieldRef with RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Any = {
    scope.getDataSourceValue(tableAlias, name).unwrapOptions
  }

  override def toSQL: String = s"$tableAlias.$name"

}
