package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.{Queryable, TableModel}
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.instructions.ReferenceInstruction
import lollypop.io.IOCost

/**
 * create table ... containing statement
 * @param ref         the provided [[DatabaseObjectRef database object reference]]
 * @param tableModel  the provided [[TableModel table]]
 * @param from        the source [[Queryable queryable]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * create table if not exists SpecialSecurities (symbol: String, lastSale: Double)
 * containing values ('AAPL', 202.11),
 *                   ('AMD', 23.50),
 *                   ('GOOG', 765.33),
 *                   ('AMZN', 699.01)
 * }}}
 */
case class CreateTableContaining(ref: DatabaseObjectRef, tableModel: TableModel, from: Queryable, ifNotExists: Boolean)
  extends ReferenceInstruction with TableCreationContaining {
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
