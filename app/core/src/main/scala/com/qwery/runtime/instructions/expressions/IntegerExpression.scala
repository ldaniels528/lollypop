package com.qwery.runtime.instructions.expressions

import com.qwery.runtime.datatypes.{DataType, Int32Type}

trait IntegerExpression extends NumericExpression {

  override def returnType: DataType = Int32Type

}