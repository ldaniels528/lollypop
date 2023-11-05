package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.{AllFields, Expression}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.{LongIntExpression, RuntimeExpression}
import com.lollypop.runtime.instructions.functions.FunctionCallParserE1
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Represents the count function
 * @param expression the [[Expression field or expression]]
 * @example {{{
 *  val stocks = tickers 10
 *  count(stocks#lastSale)
 * }}}
 */
case class Count(expression: Expression) extends AggregateFunctionCall
  with RuntimeExpression with LongIntExpression {

  override def aggregate: Aggregator = {
    var count = 0L
    new Aggregator {
      override def update(implicit scope: Scope): Unit = count += doCount

      override def collect(implicit scope: Scope): Option[Long] = Some(count)
    }
  }

  override def execute()(implicit scope: Scope): (Scope, IOCost, Long) = {
    (scope, IOCost.empty, doCount)
  }

  private def doCount(implicit scope: Scope): Long = {
    expression match {
      case AllFields => 1L
      case _ =>
        val (_, _, result) = LollypopVM.execute(scope, expression)
        result match {
          case null => 0L
          case rc: RowCollection => rc.countWhereMetadata(_.isActive)
          case _ => 1L
        }
    }
  }

}

object Count extends FunctionCallParserE1(
  name = "count",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the number of rows matching the query criteria.",
  examples = List(
    """|stocks =
       ||---------------------------------------------------------|
       || exchange | symbol | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || NYSE     | RPPI   |  51.8413 | 2023-09-28T00:58:42.974Z |
       || AMEX     | MDLA   | 177.1311 | 2023-09-28T00:58:44.363Z |
       || OTCBB    | VMUT   |          | 2023-09-28T00:58:35.392Z |
       || AMEX     | QTZUA  | 120.5353 | 2023-09-28T00:58:08.024Z |
       || OTCBB    | JCJMT  |          | 2023-09-28T00:58:17.985Z |
       || NASDAQ   | EMY    |  24.6447 | 2023-09-28T00:58:22.595Z |
       ||---------------------------------------------------------|
       |count(stocks)
       |""".stripMargin,
    """|stocks =
       ||---------------------------------------------------------|
       || exchange | symbol | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || NYSE     | RPPI   |  51.8413 | 2023-09-28T00:58:42.974Z |
       || AMEX     | MDLA   | 177.1311 | 2023-09-28T00:58:44.363Z |
       || OTCBB    | VMUT   |          | 2023-09-28T00:58:35.392Z |
       || AMEX     | QTZUA  | 120.5353 | 2023-09-28T00:58:08.024Z |
       || OTCBB    | JCJMT  |          | 2023-09-28T00:58:17.985Z |
       || NASDAQ   | EMY    |  24.6447 | 2023-09-28T00:58:22.595Z |
       ||---------------------------------------------------------|
       |count(stocks#lastSale)
       |""".stripMargin,
    """|stocks =
       ||---------------------------------------------------------|
       || exchange | symbol | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || NYSE     | RPPI   |  51.8413 | 2023-09-28T00:58:42.974Z |
       || AMEX     | MDLA   | 177.1311 | 2023-09-28T00:58:44.363Z |
       || OTCBB    | VMUT   |          | 2023-09-28T00:58:35.392Z |
       || AMEX     | QTZUA  | 120.5353 | 2023-09-28T00:58:08.024Z |
       || OTCBB    | JCJMT  |          | 2023-09-28T00:58:17.985Z |
       || NASDAQ   | EMY    |  24.6447 | 2023-09-28T00:58:22.595Z |
       ||---------------------------------------------------------|
       |select total: count(*) from @stocks
       |""".stripMargin,
    """|stocks =
       ||---------------------------------------------------------|
       || exchange | symbol | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || NYSE     | RPPI   |  51.8413 | 2023-09-28T00:58:42.974Z |
       || AMEX     | MDLA   | 177.1311 | 2023-09-28T00:58:44.363Z |
       || OTCBB    | VMUT   |          | 2023-09-28T00:58:35.392Z |
       || AMEX     | QTZUA  | 120.5353 | 2023-09-28T00:58:08.024Z |
       || OTCBB    | JCJMT  |          | 2023-09-28T00:58:17.985Z |
       || NASDAQ   | EMY    |  24.6447 | 2023-09-28T00:58:22.595Z |
       ||---------------------------------------------------------|
       |select total: count(lastSale) from @stocks
       |""".stripMargin))