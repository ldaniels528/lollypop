package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.expressions.LongIntExpression
import com.qwery.runtime.instructions.functions.FunctionCallParserE1
import com.qwery.runtime.{QweryVM, Scope}

import scala.collection.mutable

/**
 * Represents the count(unique(x)) function
 * @param expression the [[Expression field or expression]]
 */
case class CountUnique(expression: Expression) extends AggregateFunctionCall with LongIntExpression {

  override def aggregate: Aggregator = {
    val values = mutable.Set[Any]()
    new Aggregator {
      override def update(implicit scope: Scope): Unit = Option(QweryVM.execute(scope, expression)._3).foreach(values += _)

      override def collect(implicit scope: Scope): Option[Long] = Some(values.size)
    }
  }

}

object CountUnique extends FunctionCallParserE1(
  name = "countUnique",
  category = CATEGORY_DATAFRAME,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the distinct number of rows matching the query criteria.",
  examples = List(
    """|stocks =
       ||---------------------------------------------------------|
       || symbol | exchange | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || VDON   | OTCBB    |   0.4002 | 2023-07-29T05:06:56.232Z |
       || XETQ   | OTCBB    |   5.1147 | 2023-07-29T05:06:56.233Z |
       || XGDJ   | NASDAQ   |  51.5446 | 2023-07-29T05:06:56.236Z |
       || FQPY   | NASDAQ   |  75.4873 | 2023-07-29T05:06:56.238Z |
       || VNQR   | NASDAQ   |  38.5333 | 2023-07-29T05:06:56.239Z |
       ||---------------------------------------------------------|
       |select total: count(unique(exchange)) from @@stocks
       |""".stripMargin,
    """|stocks =
       ||---------------------------------------------------------|
       || symbol | exchange | lastSale | lastSaleTime             |
       ||---------------------------------------------------------|
       || VDON   | OTCBB    |   0.4002 | 2023-07-29T05:06:56.232Z |
       || XETQ   | OTCBB    |   5.1147 | 2023-07-29T05:06:56.233Z |
       || XGDJ   | NASDAQ   |  51.5446 | 2023-07-29T05:06:56.236Z |
       || FQPY   | NASDAQ   |  75.4873 | 2023-07-29T05:06:56.238Z |
       || VNQR   | NASDAQ   |  38.5333 | 2023-07-29T05:06:56.239Z |
       ||---------------------------------------------------------|
       |select total: countUnique(exchange) from @@stocks
       |""".stripMargin))