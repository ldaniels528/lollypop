package com.lollypop.runtime.instructions.expressions.aggregation

/**
 * Represents an aggregate projection
 */
trait AggregateProjection extends AggregateExpression {
  def name: String
}
