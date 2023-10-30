package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.models.Expression

/**
 * Represents an Aggregate Expression
 */
trait AggregateExpression extends Expression {

  /**
   * Provides a context for aggregation
   * @return the [[Aggregator aggregator]]
   */
  def aggregate: Aggregator

  override def isAggregation: Boolean = true

}
