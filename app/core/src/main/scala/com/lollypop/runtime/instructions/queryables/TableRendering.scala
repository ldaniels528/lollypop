package com.lollypop.runtime.instructions.queryables

import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.RowCollection

/**
 * This trait is implemented by objects that can be rendered as tables
 */
trait TableRendering {

  def toTable(implicit scope: Scope): RowCollection

  def toTableType: TableType

}