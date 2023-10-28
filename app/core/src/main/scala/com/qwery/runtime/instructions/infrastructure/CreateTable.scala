package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.qwery.language._
import com.qwery.language.models._
import com.qwery.runtime.DatabaseManagementSystem.createPhysicalTable
import com.qwery.runtime.datatypes.TableType.TableTypeRefExtensions
import com.qwery.runtime.instructions.ReferenceInstruction
import com.qwery.runtime.{DatabaseObjectRef, Scope}
import qwery.io.IOCost

import scala.collection.mutable

/**
 * create table statement
 * @param ref         the given [[DatabaseObjectRef database object reference]]
 * @param tableModel  the given [[TableModel table]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * create table Stocks (
 *    symbol: String(8),
 *    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
 *    lastSale: Double,
 *    lastSaleTime: DateTime,
 *    headlines: Table (headline: String(128), newsDate: DateTime))
 * }}}
 * @author lawrence.daniels@gmail.com
 */
case class CreateTable(ref: DatabaseObjectRef, tableModel: TableModel, ifNotExists: Boolean)
  extends RuntimeModifiable with ReferenceInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createPhysicalTable(ref.toNS, tableModel.toTableType, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("create table")
    if (ifNotExists) sb.append(" if not exists")
    sb.append(s" ${ref.toSQL} (${tableModel.columns.map(_.toSQL).mkString(",")})")
    sb.toString()
  }

}

object CreateTable extends ModifiableParser with IfNotExists {
  val templateCard: String =
    """|create table ?%IFNE:exists %L:name ?like +?%L:template ?( +?%P:columns +?) %O {{
       |?containing +?%V:source
       |?partitioned +?by +?%e:partitions
       |?initial_capacity +?is +?%e:initialCapacity
       |}}
       |""".stripMargin

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create table",
    category = CATEGORY_DATAFRAMES_INFRA,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates a persistent database table",
    example =
      """|def generateStocks(qty: Int) := {
         |  [1 to qty].map(_ => {
         |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
         |      is_otc = exchange.startsWith("OT")
         |      lastSale = scaleTo(iff(is_otc, 1, 201) * Random.nextDouble(1.0), 4)
         |      lastSaleTime = DateTime(DateTime() - Interval(1000 * 60 * Random.nextDouble(1.0)))
         |      symbol = Random.nextString(['A' to 'Z'], iff(is_otc, Random.nextInt(2) + 4, Random.nextInt(4) + 2))
         |      select lastSaleTime, lastSale, exchange, symbol
         |  }).toTable()
         |}
         |
         |namespace "temp.examples"
         |drop if exists Stocks
         |create table Stocks (symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
         |  containing (generateStocks(1000))
         |
         |graph { shape: "pie", title: "Small Caps" }
         |select exchange, total: sum(lastSale) from Stocks
         |where lastSale <= 5.0
         |group by exchange
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): RuntimeModifiable = {
    val params = SQLTemplateParams(ts, templateCard)
    val ifNotExists = params.indicators.get("exists").contains(true)
    val template = params.locations.get("template")
    val ref = params.locations("name")
    val table = TableModel(
      columns = params.parameters.getOrElse("columns", Nil).map(_.toColumn),
      initialCapacity = params.expressions.get("initialCapacity").map {
        case Literal(value: Number) => value.intValue()
        case x => dieUnsupportedType(x)
      },
      partitions = params.expressions.get("partitions"))

    // return the instruction
    template.map(CreateTableLike(ref, table, _, ifNotExists)) getOrElse {
      params.instructions.get("source") match {
        case None => CreateTable(ref, table, ifNotExists)
        case Some(from: Queryable) => CreateTableFrom(ref, table, from, ifNotExists)
        case Some(_) => ts.dieExpectedQueryable()
      }
    }
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create table"

}