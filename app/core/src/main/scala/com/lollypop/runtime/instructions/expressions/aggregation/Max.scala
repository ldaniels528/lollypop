package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.FunctionCallParserE1
import com.lollypop.runtime.{Scope, safeCast}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.util.Date

/**
 * Represents the max function
 * @param expression the [[Expression expression]]
 * @example {{{
 *  val stocks = tickers 10
 *  max(stocks#lastSale)
 * }}}
 */
case class Max(expression: Expression) extends AggregateFunctionCall
  with ColumnarFunction with RuntimeExpression {

  override def aggregate: Aggregator = {
    var maxValue_? : Option[Any] = None
    new Aggregator {
      override def update(implicit scope: Scope): Unit = {
        expression.execute(scope)._3 match {
          case value if maxValue_?.isEmpty => maxValue_? = Option(value)
          case value: Date => maxValue_? = maxValue_?.collect { case v: Date => getMax(v, value) }
          case value: Number => maxValue_? = maxValue_?.collect { case v: Number => getMax[java.lang.Double](v.doubleValue(), value.doubleValue()) }
          case value: String => maxValue_? = maxValue_?.collect { case v: String => getMax(v, value) }
          case x => expression.dieIllegalType(x)
        }
      }

      override def collect(implicit scope: Scope): Option[Any] = maxValue_?
    }
  }

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    def doIt[A <: Comparable[A]]()(implicit scope: Scope): Option[A] = {
      compute(expression, { (rc: RowCollection, columnID: Int) =>
        var maxValue_? : Option[A] = None
        rc.foreach { row =>
          val value_? = row.fields(columnID).value.flatMap(safeCast[A])
          maxValue_? = (for {minValue <- maxValue_?; value <- value_?} yield getMax(minValue, value)) ?? value_?
        }
        maxValue_?
      })
    }

    (scope, IOCost.empty, doIt().orNull)
  }

  private def getMax[A <: Comparable[A]](v1: A, v2: A): A = v1.compareTo(v2) match {
    case n if n < 0 => v2
    case n if n > 0 => v1
    case _ => v1
  }

}

object Max extends FunctionCallParserE1(
  name = "max",
  category = CATEGORY_AGG_SORT_OPS,
  paradigm = PARADIGM_FUNCTIONAL,
  description = "Returns the maximum value of a numeric expression.",
  examples = List(
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |max(stocks#lastSale)
       |""".stripMargin,
    """|val stocks =
       |    |------------------------------|
       |    | symbol | exchange | lastSale |
       |    |------------------------------|
       |    | XYZ    | AMEX     |    31.95 |
       |    | ABC    | NYSE     |    56.12 |
       |    | DNS    | AMEX     |    97.61 |
       |    |------------------------------|
       |select maxLastSale: max(lastSale) from @stocks
       |""".stripMargin))