package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.VariableRef
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

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
