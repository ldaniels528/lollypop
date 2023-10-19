package com.qwery.runtime.instructions.queryables

import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RowCollection

/**
 * This trait is implemented by objects that can be rendered as tables
 */
trait TableRendering {

  def toTable(implicit scope: Scope): RowCollection

  def toTableType: TableType

}