package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, Queryable, TableModel}
import com.lollypop.runtime.DatabaseManagementSystem.readPhysicalTable
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.{DatabaseObjectRef, Scope, Variable}
import lollypop.io.IOCost

/**
 * declare table statement
 * @param ref         the given [[Atom variable reference]]
 * @param tableModel  the given [[TableModel table model]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * declare table Stocks (
 *    symbol: String(8),
 *    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
 *    lastSale: Double,
 *    lastSaleTime: Date,
 *    headlines: Table (headline: String(128), newsDate: DateTime))
 * }}}
 * @author lawrence.daniels@gmail.com
 */
case class DeclareTable(ref: Atom, tableModel: TableModel, ifNotExists: Boolean)
  extends TableCreation {
  protected def actionVerb: String = "declare"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val _type = tableModel.toTableType
    val ns = createTempNS(_type.columns)
    val out = readPhysicalTable(ns)
    val cost = IOCost(created = 1)
    (scope.withVariable(Variable(name = ref.name, _type, initialValue = out)), cost, cost)
  }
  
}

object DeclareTable extends TableCreationParser {
  override val templateCard: String =
    """|declare table ?%IFNE:exists %a:name ?like +?%L:template ?( +?%P:columns +?) %O {{
       |?%C(_|=|containing) +?%V:source
       |?partitioned +?by +?%e:partitions
       |?initial_capacity +?is +?%e:initialCapacity
       |}}
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "declare table",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates a durable database table",
    example =
      """|declare table Stocks (
         |symbol: String(8),
         |exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
         |lastSale: Double,
         |lastSaleTime: DateTime)
         |""".stripMargin
  ))

  protected def parseTable(params: SQLTemplateParams, table: TableModel, ifNotExists: Boolean): TableCreation = {
    DeclareTable(ref = params.atoms("name"), table, ifNotExists)
  }

  protected def parseTableFrom(params: SQLTemplateParams, table: TableModel, from: Queryable, ifNotExists: Boolean): TableCreation = {
    DeclareTableFrom(ref = params.atoms("name"), table, from, ifNotExists)
  }

  protected def parseTableLike(params: SQLTemplateParams, table: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean): TableCreation = {
    DeclareTableLike(ref = params.atoms("name"), table, template, ifNotExists)
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "declare table"

}
