package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.datatypes.{DataType, Int64Type}

trait LongIntExpression extends NumericExpression {

  override def returnType: DataType = Int64Type

}
