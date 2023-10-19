package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.datatypes.{DataType, Float64Type}

trait DoubleExpression extends NumericExpression {

  override def returnType: DataType = Float64Type

}