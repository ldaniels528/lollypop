package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.runtime.Scope

/**
 * Represents an aggregate field projection
 * @param name    the output name of the result
 * @param srcName the name of the source fields
 */
case class AggregateFieldProjection(name: String, srcName: String) extends AggregateProjection {

  override def aggregate: Aggregator = {
    var value_? : Option[Any] = None
    new Aggregator {
      override def update(implicit scope: Scope): Unit = {
        value_? = scope.getCurrentRow.flatMap(_.getField(srcName)).flatMap(_.value)
      }

      override def collect(implicit scope: Scope): Option[Any] = value_?
    }
  }

}
