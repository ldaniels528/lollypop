package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.language.HelpDoc.CATEGORY_DATAFRAME
import com.qwery.language.models.Expression
import com.qwery.runtime.instructions.functions.FunctionCallParserN
import com.qwery.runtime.{QweryVM, Scope}

import scala.collection.mutable

/**
 * Represents an Unique function
 * @param expressions the given [[Expression]]s for which to aggregate the distinct values
 * @author lawrence.daniels@gmail.com
 */
case class Unique(expressions: List[Expression]) extends AggregateFunctionCall {

  override def aggregate: Aggregator = {
    val values = mutable.Set[Any]() // TODO use a device to store the values
    new Aggregator {
      override def update(implicit scope: Scope): Unit = {
        val value = expressions.headOption.flatMap(v => Option(QweryVM.execute(scope, v)._3))
        value.foreach(values += _)
      }

      override def collect(implicit scope: Scope): Option[Array[Any]] = Some(values.toArray)
    }
  }

  override def toSQL: String = s"unique${expressions.map(_.toSQL).mkString("(", ", ", ")")}"
}

/**
 * Unique Companion
 * @author lawrence.daniels@gmail.com
 */
object Unique extends FunctionCallParserN(
  name = "unique",
  category = CATEGORY_DATAFRAME,
  description = "Returns a unique collection of elements based on the query criteria.",
  example =
    """|val stocks =
       |  |------------------------------|
       |  | symbol | exchange | lastSale |
       |  |------------------------------|
       |  | XYZ    | AMEX     |    31.95 |
       |  | ABC    | NYSE     |    56.12 |
       |  | YOKE   | NYSE     |    56.12 |
       |  | DNS    | AMEX     |    97.61 |
       |  |------------------------------|
       |select exchange: unique(exchange) from @@stocks
       |""".stripMargin) {

  /**
   * Creates a new Unique expression
   * @param expressions the given [[Expression]]s for which to aggregate the unique values
   * @return a new [[Unique unique]] expression
   */
  def apply(expressions: Expression*): Unique = new Unique(expressions.toList)

}
