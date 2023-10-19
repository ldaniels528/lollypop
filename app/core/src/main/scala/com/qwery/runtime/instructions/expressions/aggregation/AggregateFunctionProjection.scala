package com.qwery.runtime.instructions.expressions.aggregation

/**
 * Represents an aggregate function projection
 * @param name the output name of the result
 * @param expr the [[AggregateFunctionCall aggregate function]]
 */
case class AggregateFunctionProjection(name: String, expr: AggregateFunctionCall)
  extends AggregateProjection {

  /**
   * Provides a context for aggregation
   * @return the [[Aggregator aggregator]]
   */
  override def aggregate: Aggregator = expr.aggregate

}
