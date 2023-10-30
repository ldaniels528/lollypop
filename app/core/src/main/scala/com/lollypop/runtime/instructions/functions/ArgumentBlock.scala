package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models._
import com.lollypop.runtime.instructions.expressions.TupleLiteral

/**
 * Represents a collection of function arguments or parameters
 */
trait ArgumentBlock extends Expression with Invokable {
  def args: List[Expression]

  def parameters: List[ParameterLike]
}

object ArgumentBlock {
  def apply(args: List[Expression]): ArgumentBlock = {
    if (args.forall(_.isInstanceOf[ParameterLike])) ParameterBlock(args.collect { case p: ParameterLike => p })
    else TupleLiteral(args)
  }

  def apply(args: Expression*): ArgumentBlock = apply(args.toList)

  def unapply(ab: ArgumentBlock): Option[List[Expression]] = Some(ab.args)
}

