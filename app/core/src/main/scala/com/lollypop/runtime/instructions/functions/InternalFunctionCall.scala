package com.lollypop.runtime.instructions.functions

import com.lollypop.language._
import com.lollypop.language.models.{Expression, FunctionCall, Instruction, NamedExpression}
import com.lollypop.util.StringRenderHelper.StringRenderer

/**
 * Represents a Lollypop-native function call
 */
trait InternalFunctionCall extends FunctionCall with NamedExpression {

  def name: String = getClass.getSimpleName.toCamelCase

  override def args: List[Expression] = this match {
    case p: Product => p.productIterator.toList.collect {
      case expr: Expression => expr
      case ins: Instruction => ins.asExpression
    }
    case _ => Nil
  }

  override def toSQL: String = {
    val myArgs = this match {
      case p: Product => p.productIterator.toList.flatMap {
        case None => None
        case Some(arg: Instruction) => Some(arg.toSQL)
        case arg: Instruction => Some(arg.toSQL)
        case arg => Some(arg.render)
      }
      case _ => Nil
    }
    Seq(name, myArgs.mkString("(", ", ", ")")).mkString
  }

}

/**
 * NativeFunctionCall Companion
 */
object InternalFunctionCall {

  def unapply(rowFx: InternalFunctionCall): Option[(String, List[Expression])] = Some(rowFx.name -> rowFx.args)

}
