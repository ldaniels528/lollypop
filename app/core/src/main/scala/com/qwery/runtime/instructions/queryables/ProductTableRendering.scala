package com.qwery.runtime.instructions.queryables

import com.qwery.runtime.plastics.RuntimeClass.implicits.RuntimeClassProduct
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.Inferences.fromClass
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.devices.{RowCollection, TableColumn}

trait ProductTableRendering extends TableRendering { self: Product =>

  override def toTable(implicit scope: Scope): RowCollection = self.toRowCollection

  def toTableType: TableType = {
    TableType(columns = self.productElementFields.map { field => TableColumn(field.getName, `type` = fromClass(field.getType)) })
  }

}
