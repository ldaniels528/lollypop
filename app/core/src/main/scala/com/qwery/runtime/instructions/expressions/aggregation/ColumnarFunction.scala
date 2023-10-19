package com.qwery.runtime.instructions.expressions.aggregation

import com.qwery.language.models.{Expression, IdentifierRef}
import com.qwery.runtime.DatabaseObjectRef.SubTable
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.instructions.queryables.HashTag
import com.qwery.runtime.{QweryVM, Scope}

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
    QweryVM.execute(scope, ref)._3 match {
      case rc: RowCollection => rc
      case _ => ref.dieIllegalType()
    }
  }

}
