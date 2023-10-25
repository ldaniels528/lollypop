package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Atom, Literal, Queryable, TableModel}
import com.qwery.language.{HelpDoc, IfNotExists, ModifiableParser, SQLCompiler, SQLTemplateParams, TokenStream, dieUnsupportedType}
import com.qwery.runtime.DatabaseManagementSystem.readPhysicalTable
import com.qwery.runtime.datatypes.TableType.TableTypeRefExtensions
import com.qwery.runtime.devices.RowCollectionZoo.createTempNS
import com.qwery.runtime.{Scope, Variable}
import qwery.io.IOCost

import scala.collection.mutable

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
case class DeclareTable(ref: Atom, tableModel: TableModel, ifNotExists: Boolean) extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val _type = tableModel.toTableType
    val ns = createTempNS(_type.columns)
    val out = readPhysicalTable(ns)
    (scope.withVariable(Variable(name = ref.name, _type, initialValue = out)), IOCost(created = 1), true)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("declare table")
    if (ifNotExists) sb.append(" if not exists")
    sb.append(s" ${ref.toSQL} (${tableModel.columns.map(_.toSQL).mkString(",")})")
    sb.toString()
  }

}

object DeclareTable extends ModifiableParser with IfNotExists {
  val templateCard: String =
    """|declare table ?%IFNE:exists %a:name ?like +?%L:template ?( +?%P:columns +?) %O {{
       |?%C(_|=|containing) +?%V:source
       |?partitioned +?by +?%e:partitions
       |?initial_capacity +?is +?%e:initialCapacity
       |}}
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "declare table",
    category = CATEGORY_DATAFRAME,
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

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): RuntimeModifiable = {
    val params = SQLTemplateParams(ts, templateCard)
    val ifNotExists = params.indicators.get("exists").contains(true)
    val template = params.locations.get("template")
    val ref = params.atoms("name")
    val table = TableModel(
      columns = params.parameters.getOrElse("columns", Nil).map(_.toColumn),
      initialCapacity = params.expressions.get("initialCapacity").map {
        case Literal(value: Number) => value.intValue()
        case x => dieUnsupportedType(x)
      },
      partitions = params.expressions.get("partitions"))

    // return the instruction
    template.map(DeclareTableLike(ref, table, _, ifNotExists)) getOrElse {
      params.instructions.get("source") match {
        case None => DeclareTable(ref, table, ifNotExists)
        case Some(from: Queryable) => DeclareTableFrom(ref, table, from, ifNotExists)
        case Some(_) => ts.dieExpectedQueryable()
      }
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "declare table"

}
