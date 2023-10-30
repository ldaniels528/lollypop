package com.lollypop.runtime.instructions.expressions

import com.lollypop.runtime.datatypes.{DataType, Int32Type}

trait IntegerExpression extends NumericExpression {

  override def returnType: DataType = Int32Type

}