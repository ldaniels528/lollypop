package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.runtime.Scope

trait Aggregator {

  /**
   * Incrementally updates the underlying aggregate value
   * @param scope the implicit [[Scope scope]]
   */
  def update(implicit scope: Scope): Unit

  /**
   * Returns the final aggregate value
   * @param scope the implicit [[Scope scope]]
   * @return the final aggregate value
   */
  def collect(implicit scope: Scope): Option[Any]

}