package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.{Expression, FunctionCall, Instruction, LambdaFunction}
import com.lollypop.runtime.datatypes.ConstructorSupport
import com.lollypop.runtime.instructions.expressions.LambdaFunctionCall
import com.lollypop.util.StringRenderHelper.StringRenderer

case class DataTypeConstructor(provider: ConstructorSupport[_]) extends LambdaFunction {

  override def call(args: List[Expression]): FunctionCall = LambdaFunctionCall(this, args)

  override def toSQL: String = provider match {
    case i: Instruction => i.toSQL
    case x => x.getClass.render
  }

}
