package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Atom, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.RowCollectionZoo.createTempTable
import com.lollypop.runtime.devices.TableColumn.implicits.TableColumnToSQLColumnConversion
import lollypop.io.IOCost

/**
 * declare table ... like statement
 * @param ref         the given [[Atom database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param template    the source/template table [[DatabaseObjectRef reference]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * declare table Stocks (
 *   symbol: String(8),
 *   exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
 *   lastSale: Double,
 *   lastSaleTime: DateTime)
 *
 * declare table PennyStocks like Stocks (rating Int)
 * }}}
 */
case class DeclareTableLike(ref: Atom, tableModel: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean)
  extends TableLikeCreation {
  protected def actionVerb: String = "declare"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val tableColumnNames = tableModel.columns.map(_.name).toSet
    val templateColumns = scope.getRowCollection(template).use(_.columns.filterNot(c => tableColumnNames.contains(c.name)))
    val aggColumns = tableModel.columns ::: templateColumns.map(_.toColumn).toList
    val _type = tableModel.copy(columns = aggColumns).toTableType
    val out = createTempTable(_type.columns)
    (scope.withVariable(Variable(name = ref.name, _type, initialValue = out)), IOCost.empty, IOCost.empty)
  }

}

