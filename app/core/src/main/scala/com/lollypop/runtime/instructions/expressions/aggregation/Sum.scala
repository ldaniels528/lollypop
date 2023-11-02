package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.Float64Type
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.{DoubleExpression, RuntimeExpression}
import com.lollypop.runtime.instructions.functions.FunctionCallParserE1
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * Represents the sum function
 * @param expression the [[Expression expression]]
 * @example {{{
 *  val stocks = tickers 10
 *  sum(stocks#lastSale)
 * }}}
 */
case class Sum(expression: Expression) extends AggregateFunctionCall
  with ColumnarFunction with RuntimeExpression with DoubleExpression {

  override def aggregate: Aggregator = {
    var sum = 0.0
    new Aggregator {
      override def update(implicit scope: Scope): Unit = sum += (expression.asDouble || 0.0)

      override def collect(implicit scope: Scope): Option[Double] = Some(sum)
    }
  }

  override def execute()(implicit scope: Scope): (Scope, IOCost, Double) = {
    val result = compute(expression, { (rc: RowCollection, columnID: Int) =>
      var sum: Double = 0
      rc.foreach { row => sum += Float64Type.convert(row.fields(columnID).value) }
      sum
    })
    (scope, IOCost.empty, result)
  }

}

object Sum extends FunctionCallParserE1(
  name = "sum",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the sum of a numeric expression.",
  examples = List(
    """|stocks = (
       ||-------------------|
       || symbol | lastSale |
       ||-------------------|
       || VHLH   | 153.2553 |
       || GPI    |  89.7307 |
       || SGE    | 131.6038 |
       || GVABB  |  31.1324 |
       || GTIT   | 110.6881 |
       || JDXEZ  | 243.4389 |
       || RNUBE  | 157.2571 |
       || DBY    | 237.5894 |
       || CO     | 109.6587 |
       || BIU    | 232.9175 |
       ||-------------------|
       |)
       |sum(stocks#lastSale)
       |""".stripMargin,
    """|select total: sum(lastSale) from (
       ||-------------------|
       || symbol | lastSale |
       ||-------------------|
       || VHLH   | 153.2553 |
       || GPI    |  89.7307 |
       || SGE    | 131.6038 |
       || GVABB  |  31.1324 |
       || GTIT   | 110.6881 |
       || JDXEZ  | 243.4389 |
       || RNUBE  | 157.2571 |
       || DBY    | 237.5894 |
       || CO     | 109.6587 |
       || BIU    | 232.9175 |
       ||-------------------|
       |)
       |""".stripMargin))