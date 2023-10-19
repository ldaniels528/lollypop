package com.qwery.runtime.instructions.queryables

import com.qwery.language.models.Queryable
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Represents a union distinct operation; which combines two queries into a distinct set.
 * @param query0 the first [[Queryable queryable resource]]
 * @param query1 the second [[Queryable queryable resource]]
 */
case class UnionDistinct(query0: Queryable, query1: Queryable) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = QweryVM.search(scope, query0)
    val (scopeB, costB, deviceB) = QweryVM.search(scopeA, query1)
    (scopeB, costA ++ costB, deviceA unionDistinct deviceB)
  }

  override def toSQL: String = s"${query0.toSQL} union distinct ${query1.toSQL}"

}
