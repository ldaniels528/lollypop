package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.{BinaryQueryable, Queryable}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a union distinct operation; which combines two queries into a distinct set.
 * @param a the first [[Queryable queryable resource]]
 * @param b the second [[Queryable queryable resource]]
 */
case class UnionDistinct(a: Queryable, b: Queryable) extends RuntimeQueryable with BinaryQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = a.search(scope)
    val (scopeB, costB, deviceB) = b.search(scopeA)
    (scopeB, costA ++ costB, deviceA unionDistinct deviceB)
  }

  override def toSQL: String = s"${a.toSQL} union distinct ${b.toSQL}"

}
