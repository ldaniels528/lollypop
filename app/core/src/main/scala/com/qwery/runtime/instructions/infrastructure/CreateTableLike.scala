package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.TableModel
import com.qwery.runtime.datatypes.TableType.TableTypeRefExtensions
import com.qwery.runtime.devices.TableColumn.implicits.TableColumnToSQLColumnConversion
import com.qwery.runtime.instructions.ReferenceInstruction
import com.qwery.runtime.{DatabaseManagementSystem, DatabaseObjectRef, Scope}
import com.qwery.util.ResourceHelper._
import qwery.io.IOCost

import scala.collection.mutable

/**
 * create table ... like statement
 * @param ref         the given [[DatabaseObjectRef database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param template    the source [[DatabaseObjectRef reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class CreateTableLike(ref: DatabaseObjectRef, tableModel: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean)
  extends RuntimeModifiable with ReferenceInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val tableColumnNames = tableModel.columns.map(_.name).toSet
    val templateColumns = scope.getRowCollection(template).use(_.columns.filterNot(c => tableColumnNames.contains(c.name)))
    val aggColumns = tableModel.columns ::: templateColumns.map(_.toColumn).toList
    (scope, DatabaseManagementSystem.createPhysicalTable(ref.toNS, tableModel.copy(columns = aggColumns).toTableType, ifNotExists), true)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("create table")
    if (ifNotExists) sb.append(" if not exists")
    sb.append(s" ${ref.toSQL} like ${template.toSQL} (${tableModel.columns.map(_.toSQL).mkString(",")})")
    sb.toString()
  }

}
