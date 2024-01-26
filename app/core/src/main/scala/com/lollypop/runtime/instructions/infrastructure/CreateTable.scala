package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_INFRA, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.DatabaseManagementSystem.createPhysicalTable
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.instructions.ReferenceInstruction
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

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
  extends ReferenceInstruction with TableCreation {
  override protected def actionVerb: String = "create"

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createPhysicalTable(ref.toNS, tableModel.toTableType, ifNotExists)
    (scope, cost, cost)
  }

}

object CreateTable extends TableCreationParser {
  override val templateCard: String =
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
         |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
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
         |graph { shape: "pie3d", title: "Small Caps" }
         |select exchange, total: sum(lastSale) from Stocks
         |where lastSale <= 5.0
         |group by exchange
         |""".stripMargin
  ))

  protected def parseTable(params: SQLTemplateParams, table: TableModel, ifNotExists: Boolean): TableCreation = {
    CreateTable(ref = params.locations("name"), table, ifNotExists)
  }

  protected def parseTableFrom(params: SQLTemplateParams, table: TableModel, from: Queryable, ifNotExists: Boolean): TableCreation = {
    CreateTableFrom(ref = params.locations("name"), table, from, ifNotExists)
  }

  protected def parseTableLike(params: SQLTemplateParams, table: TableModel, template: DatabaseObjectRef, ifNotExists: Boolean): TableCreation = {
    CreateTableLike(ref = params.locations("name"), table, template, ifNotExists)
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create table"

}