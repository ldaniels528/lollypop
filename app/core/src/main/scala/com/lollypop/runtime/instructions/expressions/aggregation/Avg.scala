package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{DataType, Float64Type, Inferences}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.{DoubleExpression, RuntimeExpression}
import com.lollypop.runtime.instructions.functions.FunctionCallParserE1
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.util.Date

/**
 * Represents the avg function
 * @param expression the [[Expression expression]]
 * @example {{{
 *  val stocks = tickers 10
 *  avg(stocks#lastSale)
 * }}}
 */
case class Avg(expression: Expression) extends AggregateFunctionCall
  with ColumnarFunction with RuntimeExpression with DoubleExpression {
  private var returnType_? : Option[DataType] = None

  override def aggregate: Aggregator = {
    var (sum, count) = (0.0, 0L)
    new Aggregator {
      override def update(implicit scope: Scope): Unit = {
        val value = expression.asAny
        if (returnType_?.isEmpty) returnType_? = Option(value).map(Inferences.fromValue)
        sum += expression.asAny.map {
          case char: Char => char.toDouble
          case date: Date => date.getTime.toDouble
          case number: Number => number.doubleValue()
          case x => expression.dieIllegalType(x)
        } || 0.0
        count += 1
      }

      override def collect(implicit scope: Scope): Option[Any] = if (count > 0) returnType_?.map(_.convert(sum / count)) else None
    }
  }

  override def execute()(implicit scope: Scope): (Scope, IOCost, Double) = {
    compute(expression, { (rc, columnID) =>
      var sum = 0.0
      var count = 0L
      rc.foreach { row =>
        sum += Float64Type.convert(row.fields(columnID).value)
        count += 1
      }
      (scope, IOCost.empty, sum / count)
    })
  }

}

object Avg extends FunctionCallParserE1(
  name = "avg",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Computes the average of a numeric expression.",
  examples = List(
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |avg(stocks#lastSale)
       |""".stripMargin,
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |select avgLastSale: avg(lastSale) from @stocks
       |""".stripMargin))
