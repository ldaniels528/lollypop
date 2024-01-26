package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Queryable, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.{Field, FieldMetadata}
import com.lollypop.runtime.instructions.ReferenceInstruction
import com.lollypop.runtime.instructions.queryables.RowsOfValues
import lollypop.io.IOCost

/**
 * create table ... from statement
 * @param ref         the given [[DatabaseObjectRef database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param from        the given [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateTableFrom(ref: DatabaseObjectRef, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends ReferenceInstruction with TableCreation {
  override protected def actionVerb: String = "create"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    // attempt to create the table
    val cost0 = DatabaseManagementSystem.createPhysicalTable(ref.toNS, tableModel.toTableType, ifNotExists)

    // truncate the table (if not empty)
    val device = scope.getRowCollection(ref)
    if (device.getLength > 0) device.setLength(newSize = 0)

    // insert the rows
    val cost1 = from match {
      case RowsOfValues(values) =>
        val fields = tableModel.columns.map(c => Field(name = c.name, metadata = FieldMetadata(), value = c.defaultValue))
        device.insertRows(fields.map(_.name), values)
      case queryable =>
        val (_, cost2, device1) = queryable.search(scope)
        cost2 ++ device.insert(device1)
    }

    val cc = cost0 ++ cost1
    (scope, cc, cc)
  }

  override def toSQL: String = s"${super.toSQL} containing ${from.toSQL}"

}
