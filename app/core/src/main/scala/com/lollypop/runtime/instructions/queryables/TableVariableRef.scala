package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.VariableRef
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

/**
 * Represents a reference to a table variable
 * @param name the name of the table variable
 */
case class TableVariableRef(name: String) extends VariableRef with DatabaseObjectRef with RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    (scope, IOCost.empty, scope.resolveTableVariable(name))
  }

  override def toSQL: String = s"@@$name"

}
