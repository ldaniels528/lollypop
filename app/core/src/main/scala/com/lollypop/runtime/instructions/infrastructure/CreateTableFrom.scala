package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Queryable, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.instructions.ReferenceInstruction
import lollypop.io.IOCost

/**
 * create table ... from statement
 * @param ref         the provided [[DatabaseObjectRef database object reference]]
 * @param tableModel  the provided [[TableModel table]]
 * @param from        the source [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateTableFrom(ref: DatabaseObjectRef, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends ReferenceInstruction with TableCreationFrom {
  override protected def actionVerb: String = "create"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    // attempt to create the table
    val cost0 = DatabaseManagementSystem.createPhysicalTable(ref.toNS, tableModel.toTableType, ifNotExists)

    // truncate the table (if not empty)
    val device = scope.getRowCollection(ref)
    if (device.getLength > 0) device.setLength(newSize = 0)

    // insert the rows
    val cost1 = insertRows(device)

    val cc = cost0 ++ cost1
    (scope, cc, cc)
  }

}
