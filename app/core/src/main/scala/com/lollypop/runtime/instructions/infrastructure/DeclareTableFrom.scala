package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Atom, Queryable, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.RowCollectionZoo.createTempTable
import lollypop.io.IOCost

/**
 * declare table ... from statement
 * @param ref         the provided [[Atom database object reference]]
 * @param tableModel  the provided [[TableModel table]]
 * @param from        the source [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class DeclareTableFrom(ref: Atom, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends TableCreationFrom {
  protected def actionVerb: String = "declare"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    // attempt to declare the table
    val _type = tableModel.toTableType
    val device = createTempTable(_type.columns)

    // insert the rows
    val cost1 = insertRows(device)

    (scope.withVariable(Variable(name = ref.name, _type, initialValue = device)), cost1, cost1)
  }

}
