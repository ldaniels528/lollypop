package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Atom, Queryable, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.RowCollectionZoo.createTempTable
import com.lollypop.runtime.devices.{Field, FieldMetadata}
import com.lollypop.runtime.instructions.queryables.RowsOfValues
import lollypop.io.IOCost

import scala.collection.mutable

/**
 * declare table ... from statement
 * @param ref         the given [[Atom database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param from        the given [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @author lawrence.daniels@gmail.com
 */
case class DeclareTableFrom(ref: Atom, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    // attempt to declare the table
    val _type = tableModel.toTableType
    val out = createTempTable(_type.columns)

    // insert the rows
    val cost1 = from match {
      case RowsOfValues(values) =>
        val fields = tableModel.columns.map(c => Field(name = c.name, metadata = FieldMetadata(), value = c.defaultValue))
        out.insertRows(fields.map(_.name), values)
      case queryable =>
        val (_, cost2, device1) = queryable.search(scope)
        cost2 ++ out.insert(device1)
    }

    (scope.withVariable(Variable(name = ref.name, _type, initialValue = out)), cost1, cost1)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("declare table")
    if (ifNotExists) sb.append(" if not exists")
    sb.append(s" ${ref.toSQL} (${tableModel.columns.map(_.toSQL).mkString(",")}) containing ${from.toSQL}")
    sb.toString()
  }

}
