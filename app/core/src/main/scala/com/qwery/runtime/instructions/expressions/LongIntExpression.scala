package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.datatypes.{DataType, Int64Type}

trait LongIntExpression extends NumericExpression {

  override def returnType: DataType = Int64Type

}
