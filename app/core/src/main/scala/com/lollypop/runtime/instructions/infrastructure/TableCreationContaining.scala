package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.Queryable
import com.lollypop.runtime.devices.{Field, FieldMetadata, RowCollection}
import com.lollypop.runtime.instructions.queryables.RowsOfValues
import com.lollypop.runtime.{LollypopVMAddOns, Scope}
import lollypop.io.IOCost

/**
 * Table Creation From Trait
 */
trait TableCreationContaining extends TableCreation {

  /**
   * @return the source [[Queryable queryable]]
   */
  def from: Queryable

  override def toSQL: String = s"${super.toSQL} containing ${from.toSQL}"

  protected def insertRows(device: RowCollection)(implicit scope: Scope): IOCost = from match {
    case RowsOfValues(values) =>
      val fields = tableModel.columns.map(c => Field(name = c.name, metadata = FieldMetadata(), value = c.defaultValue))
      device.insertRows(fields.map(_.name), values)
    case queryable =>
      val (_, ca, rc) = queryable.search(scope)
      ca ++ device.insert(rc)
  }

}
