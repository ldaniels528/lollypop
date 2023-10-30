package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.datatypes.{DataType, Float64Type}

trait DoubleExpression extends NumericExpression {

  override def returnType: DataType = Float64Type

}