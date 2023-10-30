package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.functions.FunctionCallParserE1
import com.lollypop.runtime.{Scope, safeCast}
import com.lollypop.util.OptionHelper.OptionEnrichment

import java.util.Date

/**
 * Represents the min function
 * @param expression the [[Expression expression]]
 * @example {{{
 *  val stocks = tickers 10
 *  min(stocks#lastSale)
 * }}}
 */
case class Min(expression: Expression) extends AggregateFunctionCall with ColumnarFunction with RuntimeExpression {

  override def aggregate: Aggregator = {
    var minValue_? : Option[Any] = None
    new Aggregator {
      override def update(implicit scope: Scope): Unit = {
        expression.asAny
          .foreach {
            case value if minValue_?.isEmpty => minValue_? = Option(value)
            case value: Date => minValue_? = minValue_?.collect { case v: Date => getMin(v, value) }
            case value: Number => minValue_? = minValue_?.collect { case v: Number => getMin[java.lang.Double](v.doubleValue(), value.doubleValue()) }
            case value: String => minValue_? = minValue_?.collect { case v: String => getMin(v, value) }
            case x => expression.dieIllegalType(x)
          }
      }

      override def collect(implicit scope: Scope): Option[Any] = minValue_?
    }
  }

  override def evaluate()(implicit scope: Scope): Any = doIt().orNull

  private def doIt[A <: Comparable[A]]()(implicit scope: Scope): Option[A] = {
    compute(expression, { (rc: RowCollection, columnID: Int) =>
      var minValue_? : Option[A] = None
      rc.foreach { row =>
        val value_? = row.fields(columnID).value.flatMap(safeCast[A])
        minValue_? = (for {minValue <- minValue_?; value <- value_?} yield getMin(minValue, value)) ?? value_?
      }
      minValue_?
    })
  }

  private def getMin[A <: Comparable[A]](v1: A, v2: A): A = v1.compareTo(v2) match {
    case n if n < 0 => v1
    case n if n > 0 => v2
    case _ => v1
  }

}

object Min extends FunctionCallParserE1(
  name = "min",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the minimum value of a numeric expression.",
  examples = List(
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |min(stocks#lastSale)
       |""".stripMargin,
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |select minLastSale: min(lastSale) from @@stocks
       |""".stripMargin))
