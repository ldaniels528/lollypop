package com.lollypop.runtime.instructions.queryables

import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{Scope, _}

trait ProductTableRendering extends TableRendering { self: Product =>

  override def toTable(implicit scope: Scope): RowCollection = self.toRowCollection

  def toTableType: TableType = self.asTableType

}
