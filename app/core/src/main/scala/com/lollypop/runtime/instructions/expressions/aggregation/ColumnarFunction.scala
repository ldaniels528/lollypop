package com.lollypop.runtime.instructions.expressions.aggregation

import com.lollypop.language.models.{Expression, IdentifierRef}
import com.lollypop.runtime.DatabaseObjectRef.SubTable
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.queryables.HashTag
import com.lollypop.runtime.{Scope, _}

/**
 * Columnar Function
 */
trait ColumnarFunction {

  def compute[A](expression: Expression, fx: (RowCollection, Int) => A)(implicit scope: Scope): A = {
    expression match {
      case HashTag(ref, IdentifierRef(columnName)) =>
        val rc = resolve(ref)
        fx(rc, rc.getColumnIdByNameOrDie(columnName))
      case SubTable(ref, columnName) =>
        val rc = resolve(ref)
        fx(rc, rc.getColumnIdByNameOrDie(columnName))
      case ref => ref.die("No Column was not specified")
    }
  }

  private def resolve(ref: Expression)(implicit scope: Scope): RowCollection = {
    ref.execute(scope)._3 match {
      case rc: RowCollection => rc
      case _ => ref.dieIllegalType()
    }
  }

}
