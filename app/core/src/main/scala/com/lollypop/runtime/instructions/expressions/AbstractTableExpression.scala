package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.TableColumn

abstract class AbstractTableExpression(columns: Seq[TableColumn]) extends TableExpression {

  override def returnType: TableType = TableType(columns)

}
