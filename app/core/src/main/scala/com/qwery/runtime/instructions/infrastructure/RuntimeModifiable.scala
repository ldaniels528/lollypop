package com.qwery.runtime.instructions.infrastructure

import com.qwery.language.models.Modifiable
import com.qwery.runtime.datatypes.TableType
import com.qwery.runtime.devices.RowCollectionZoo.ProductClassToTableType
import com.qwery.runtime.instructions.RuntimeInstruction
import com.qwery.runtime.instructions.expressions.TableExpression
import qwery.io.IOCost

/**
 * Represents a run-time infrastructural modification
 */
trait RuntimeModifiable extends Modifiable with RuntimeInstruction with TableExpression {

  override def returnType: TableType = classOf[IOCost].toTableType

}
