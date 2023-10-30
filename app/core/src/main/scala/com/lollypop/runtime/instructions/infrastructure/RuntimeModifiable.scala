package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.models.Modifiable
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.RowCollectionZoo.ProductClassToTableType
import com.lollypop.runtime.instructions.RuntimeInstruction
import com.lollypop.runtime.instructions.expressions.TableExpression
import lollypop.io.IOCost

/**
 * Represents a run-time infrastructural modification
 */
trait RuntimeModifiable extends Modifiable with RuntimeInstruction with TableExpression {

  override def returnType: TableType = classOf[IOCost].toTableType

}
