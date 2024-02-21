package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.TableModel
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.TableColumn.implicits.TableColumnToSQLColumnConversion
import com.lollypop.runtime.instructions.ReferenceInstruction
import lollypop.io.IOCost

/**
 * create table ... like statement
 * @param ref         the given [[DatabaseObjectRef database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param template    the source/template table [[DatabaseObjectRef reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * create table Stocks (
 *   symbol: String(8),
 *   exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
 *   lastSale: Double,
 *   lastSaleTime: DateTime)
 *
 * create table PennyStocks like Stocks (rating Int)
 * }}}
 */
case class CreateTableLike(ref: DatabaseObjectRef, tableModel: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean)
  extends ReferenceInstruction with TableLikeCreation {
  override protected def actionVerb: String = "create"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val tableColumnNames = tableModel.columns.map(_.name).toSet
    val templateColumns = scope.getRowCollection(template).use(_.columns.filterNot(c => tableColumnNames.contains(c.name)))
    val aggColumns = tableModel.columns ::: templateColumns.map(_.toColumn).toList
    val cost = DatabaseManagementSystem.createPhysicalTable(ref.toNS, tableModel.copy(columns = aggColumns).toTableType, ifNotExists)
    (scope, cost, cost)
  }

}
