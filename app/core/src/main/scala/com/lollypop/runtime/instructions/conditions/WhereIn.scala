package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Condition, Expression, FieldRef}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.{Row, RowCollection}
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.conditions.WhereIn.keyword
import lollypop.io.IOCost

/**
 * Represents a WhereIn clause
 * @param field     the source [[FieldRef field]]
 * @param condition the inclusion [[Condition condition]]
 * @example {{{
 * select * from Stocks
 * where symbol is 'AAPL'
 * and transactions wherein (id < 5)
 * and keyStatistics wherein (beta >= 0.5 and warrants wherein (message matches '%clear%'))
 * }}}
 */
case class WhereIn(field: Expression, condition: Condition) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val result = (for {
      srcRow <- scope.getCurrentRow
      srcField <- srcRow.getField(field.getNameOrDie)
      srcTable <- srcField.value.collect { case rc: RowCollection => rc }
    } yield {
      var matched = false
      srcTable.iterateWhere(condition = condition, limit = 1.v)(_.isActive) { (_: Scope, _: Row) => matched = true }
      matched
    }).contains(true)
    (scope, IOCost.empty, result)
  }

  override def toSQL: String = Seq(field.toSQL, keyword, condition.toSQL).mkString(" ")
}

object WhereIn extends ExpressionToConditionPostParser {
  private val keyword = "wherein"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[WhereIn] = {
    if (ts.nextIf(keyword)) compiler.nextCondition(ts).map(WhereIn(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|stocks = Table(symbol: String(4), exchange: String(8), transactions: Table(price: Double, transactionTime: DateTime)[2])
         |insert into @stocks (symbol, exchange, transactions)
         |values ('AAPL', 'NASDAQ', {price: 156.39, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('AMD',  'NASDAQ', {price:  56.87, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('INTC', 'NYSE',   {price:  89.44, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('AMZN', 'NASDAQ', {price: 988.12, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('SHMN', 'OTCBB', [{price: 0.0010, transactionTime: "2021-08-05T19:23:11.000Z"},
         |                          {price: 0.0011, transactionTime: "2021-08-05T19:23:12.000Z"}])
         |
         |select unnest(transactions)
         |from @stocks
         |where transactions wherein (price is 0.0011)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}