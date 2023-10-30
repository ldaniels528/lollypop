package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.FieldRef
import com.lollypop.runtime.LollypopVM.implicits.RichScalaAny
import com.lollypop.runtime.Scope

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
