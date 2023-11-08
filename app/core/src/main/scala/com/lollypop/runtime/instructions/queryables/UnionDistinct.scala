package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Queryable
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.RowCollection
import lollypop.io.IOCost

/**
 * Represents a union distinct operation; which combines two queries into a distinct set.
 * @param query0 the first [[Queryable queryable resource]]
 * @param query1 the second [[Queryable queryable resource]]
 */
case class UnionDistinct(query0: Queryable, query1: Queryable) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = query0.search(scope)
    val (scopeB, costB, deviceB) = query1.search(scopeA)
    (scopeB, costA ++ costB, deviceA unionDistinct deviceB)
  }

  override def toSQL: String = s"${query0.toSQL} union distinct ${query1.toSQL}"

}
