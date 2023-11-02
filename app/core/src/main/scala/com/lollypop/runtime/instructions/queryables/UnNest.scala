package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.functions.{FunctionCallParserE1, ScalarFunctionCall}
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Un-nests an inner-table within a selection into multiple rows.
 * @example {{{ unnest(transactions) }}}
 * @param expression the [[Expression expression]] to unnest
 */
case class UnNest(expression: Expression) extends ScalarFunctionCall with RuntimeQueryable {
  override val functionName: String = "unnest"

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = LollypopVM.search(scope, expression)
}

object UnNest extends FunctionCallParserE1(
  name = "unnest",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_DECLARATIVE,
  description =
    "Separates the elements of a collection expression into multiple rows, or the elements of map expr into multiple rows and columns.",
  example =
    """|declare table stocks(symbol: String(4), exchange: String(6), transactions: Table(price: Double, transactionTime: DateTime)[5])
       |insert into @@stocks (symbol, exchange, transactions)
       |values ('AAPL', 'NASDAQ', {price:156.39, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('AMD',  'NASDAQ', {price:56.87, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('INTC', 'NYSE',   {price:89.44, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('AMZN', 'NASDAQ', {price:988.12, transactionTime:"2021-08-05T19:23:11.000Z"}),
       |       ('SHMN', 'OTCBB', [{price:0.0010, transactionTime:"2021-08-05T19:23:11.000Z"},
       |                          {price:0.0011, transactionTime:"2021-08-05T19:23:12.000Z"}])
       |
       |select symbol, exchange, unnest(transactions) from @@stocks where symbol is 'SHMN'
       |""".stripMargin)