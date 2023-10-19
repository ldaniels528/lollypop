package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Column
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.runtime.instructions.queryables.RuntimeQueryable
import qwery.io.IOCost

/**
 * table statement
 * @param columns the table [[Column columns]]
 * @example {{{
 * table Stocks (
 *    symbol: String(8),
 *    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
 *    lastSale: Double,
 *    lastSaleTime: DateTime,
 *    headlines: Table (headline: String(128), newsDate: DateTime))
 * }}}
 * @author lawrence.daniels@gmail.com
 */
case class Table(columns: List[Column]) extends ScalarFunctionCall with RuntimeQueryable {
  override val functionName: String = "Table"

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    (scope, IOCost(created = 1), createQueryResultTable(columns.map(_.toTableColumn)))
  }

  override def toSQL: String = List(functionName, columns.map(_.toSQL).mkString("(", ", ", ")")).mkString
}

object Table extends FunctionCallParserP(
  name = "Table",
  category = CATEGORY_DATAFRAME,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns a new transient table",
  example =
    """|val stocks = Table(symbol: String(4), exchange: String(6), transactions: Table(price: Double, transactionTime: DateTime)[5])
       |insert into @@stocks (symbol, exchange, transactions)
       |values ('AAPL', 'NASDAQ', {price:156.39, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('AMD',  'NASDAQ', {price:56.87, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('INTC', 'NYSE',   {price:89.44, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('AMZN', 'NASDAQ', {price:988.12, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('SHMN', 'OTCBB', [{price:0.0010, transactionTime:"2021-08-05T19:23:11.000Z"},
       |                          {price:0.0011, transactionTime:"2021-08-05T19:23:12.000Z"}])
       |@@stocks
       |""".stripMargin)

