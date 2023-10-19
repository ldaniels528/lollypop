package com.qwery.runtime.instructions.functions

import com.qwery.language.models.{Expression, FunctionCall, Instruction, LambdaFunction}
import com.qwery.runtime.datatypes.ConstructorSupport
import com.qwery.runtime.instructions.expressions.LambdaFunctionCall
import com.qwery.util.StringRenderHelper.StringRenderer

case class DataTypeConstructor(provider: ConstructorSupport[_]) extends LambdaFunction {

  override def call(args: List[Expression]): FunctionCall = LambdaFunctionCall(this, args)

  override def toSQL: String = provider match {
    case i: Instruction => i.toSQL
    case x => x.getClass.render
  }

}
