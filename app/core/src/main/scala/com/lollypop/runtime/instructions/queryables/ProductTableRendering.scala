package com.lollypop.runtime.instructions.queryables

import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.Inferences.fromClass
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.plastics.RuntimeClass.implicits.RuntimeClassProduct

trait ProductTableRendering extends TableRendering { self: Product =>

  override def toTable(implicit scope: Scope): RowCollection = self.toRowCollection

  def toTableType: TableType = {
    TableType(columns = self.productElementFields.map { field => TableColumn(field.getName, `type` = fromClass(field.getType)) })
  }

}
