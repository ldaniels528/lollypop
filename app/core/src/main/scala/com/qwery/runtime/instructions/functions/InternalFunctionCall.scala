package com.qwery.runtime.instructions.functions

import com.qwery.language.models.{Expression, FunctionCall, Instruction}
import com.qwery.util.StringHelper.StringEnrichment
import com.qwery.util.StringRenderHelper.StringRenderer

/**
 * Represents an internal function call
 */
trait InternalFunctionCall extends FunctionCall {
  val functionName: String = getClass.getSimpleName.toCamelCase

  override def args: List[Expression] = this match {
    case p: Product => p.productIterator.toList.collect { case expr: Expression => expr }
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
    Seq(functionName, myArgs.mkString("(", ", ", ")")).mkString
  }

}

/**
 * NativeFunction Companion
 */
object InternalFunctionCall {

  def unapply(rowFx: InternalFunctionCall): Option[(String, List[Expression])] = Some(rowFx.functionName -> rowFx.args)

}
