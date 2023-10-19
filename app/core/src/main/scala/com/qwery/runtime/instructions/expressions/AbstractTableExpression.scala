package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.TableColumn

abstract class AbstractTableExpression(columns: Seq[TableColumn]) extends TableExpression {

  override def returnType: TableType = TableType(columns)

}
