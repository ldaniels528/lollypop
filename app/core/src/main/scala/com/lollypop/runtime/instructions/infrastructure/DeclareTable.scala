package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Atom, Literal, Queryable, TableModel}
import com.lollypop.runtime.DatabaseManagementSystem.readPhysicalTable
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.{Scope, Variable}
import lollypop.io.IOCost

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

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val _type = tableModel.toTableType
    val ns = createTempNS(_type.columns)
    val out = readPhysicalTable(ns)
    val cost = IOCost(created = 1)
    (scope.withVariable(Variable(name = ref.name, _type, initialValue = out)), cost, cost)
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

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[RuntimeModifiable] = {
    if (understands(ts)) {
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
      val op_? = template.map(DeclareTableLike(ref, table, _, ifNotExists))
      if (op_?.nonEmpty) op_? else {
        params.instructions.get("source") match {
          case None => Some(DeclareTable(ref, table, ifNotExists))
          case Some(from: Queryable) => Some(DeclareTableFrom(ref, table, from, ifNotExists))
          case Some(_) => ts.dieExpectedQueryable()
        }
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "declare table"

}
