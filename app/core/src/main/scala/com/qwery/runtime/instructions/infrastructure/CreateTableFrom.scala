package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.{Queryable, TableModel}
import com.qwery.runtime.datatypes.TableType.TableTypeRefExtensions
import com.qwery.runtime.devices.{Field, FieldMetadata}
import com.qwery.runtime.instructions.ReferenceInstruction
import com.qwery.runtime.instructions.queryables.RowsOfValues
import com.qwery.runtime.{DatabaseManagementSystem, DatabaseObjectRef, QweryVM, Scope}
import qwery.io.IOCost

import scala.collection.mutable

/**
 * create table ... from statement
 * @param ref         the given [[DatabaseObjectRef database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param from        the given [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateTableFrom(ref: DatabaseObjectRef, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends RuntimeModifiable with ReferenceInstruction {

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
        val (_, cost2, device1) = QweryVM.search(scope, queryable)
        cost2 ++ device.insert(device1)
    }

    val cc = cost0 ++ cost1
    (scope, cc, cc)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("create table")
    if (ifNotExists) sb.append(" if not exists")
    sb.append(s" ${ref.toSQL} (${tableModel.columns.map(_.toSQL).mkString(",")}) containing ${from.toSQL}")
    sb.toString()
  }

}
