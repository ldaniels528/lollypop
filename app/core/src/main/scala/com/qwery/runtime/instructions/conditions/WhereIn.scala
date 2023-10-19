package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression.implicits.{LifestyleExpressionsAny, RichAliasable}
import com.qwery.language.models.{Condition, Expression, FieldRef}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.{Row, RowCollection}
import com.qwery.runtime.instructions.conditions.WhereIn.__name
import com.qwery.util.OptionHelper.implicits.risky._

/**
 * Represents a WhereIn clause
 * @param field     the source [[FieldRef field]]
 * @param condition the inclusion [[Condition condition]]
 * @example {{{
 * select * from Stocks
 * where symbol is 'AAPL'
 * and transactions wherein (id < 5)
 * and keyStatistics wherein (beta >= 0.5 and warrants wherein (message like '%clear%'))
 * }}}
 */
case class WhereIn(field: Expression, condition: Condition) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    (for {
      srcRow <- scope.getCurrentRow
      srcField <- srcRow.getField(field.getNameOrDie)
      srcTable <- srcField.value.collect { case rc: RowCollection => rc }
    } yield {
      var matched = false
      srcTable.iterateWhere(condition = condition, limit = 1.v)(_.isActive) { (_: Scope, _: Row) => matched = true }
      matched
    }).contains(true)
  }

  override def toSQL: String = Seq(field.toSQL, __name, condition.toSQL).mkString(" ")
}

object WhereIn extends ExpressionToConditionPostParser {
  private val __name = "wherein"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[WhereIn] = {
    if (ts.nextIf(__name)) compiler.nextCondition(ts).map(WhereIn(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` ${__name} `expression`",
    description = "determines whether the `value` contains the `expression`",
    example =
      """|stocks = Table(symbol: String(4), exchange: String(8), transactions: Table(price: Double, transactionTime: DateTime)[2])
         |insert into @@stocks (symbol, exchange, transactions)
         |values ('AAPL', 'NASDAQ', {price: 156.39, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('AMD',  'NASDAQ', {price:  56.87, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('INTC', 'NYSE',   {price:  89.44, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('AMZN', 'NASDAQ', {price: 988.12, transactionTime: "2021-08-05T19:23:11.000Z"}),
         |       ('SHMN', 'OTCBB', [{price: 0.0010, transactionTime: "2021-08-05T19:23:11.000Z"},
         |                          {price: 0.0011, transactionTime: "2021-08-05T19:23:12.000Z"}])
         |
         |select unnest(transactions)
         |from @@stocks
         |where transactions wherein (price is 0.0011)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}