package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.{Instruction, Queryable}
import com.lollypop.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.lollypop.runtime.devices.{Row, RowCollection}
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

case class AssumeQueryable(instruction: Instruction) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope0, cost0, result0) = LollypopVM.execute(scope, instruction)
    result0 match {
      case rc: RowCollection => (scope0, cost0, rc)
      case row: Row => (scope0, cost0, row.toRowCollection)
      case sc: Scope => (scope0, cost0, sc.toRowCollection)
      case tr: TableRendering => (scope0, cost0, tr.toTable)
      case x => instruction.dieIllegalType(x)
    }
  }

  override def toSQL: String = instruction.toSQL

}

object AssumeQueryable {
  final implicit class EnrichedAssumeQueryable(val instruction: Instruction) extends AnyVal {
    @inline
    def asQueryable: Queryable = {
      instruction match {
        case queryable: Queryable => queryable
        case other => new AssumeQueryable(other)
      }
    }
  }
}
