package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.language.models.{Expression, ParameterLike}

/**
 * Represents a collection of function parameters
 */
case class ParameterBlock(parameters: List[ParameterLike]) extends ArgumentBlock {
  override def args: List[Expression] = parameters.map(_.name.f)

  override def toSQL: String = parameters.map(_.toSQL).mkString("(", ", ", ")")
}

object ParameterBlock {
  def apply(args: ParameterLike*) = new ParameterBlock(args.toList)
}
