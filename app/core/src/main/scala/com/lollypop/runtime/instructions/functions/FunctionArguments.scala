package com.lollypop.runtime.instructions.functions

import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.instructions.expressions.TupleLiteral

/**
 * Utility that provides an <code>unapply</code> for returning function arguments
 */
object FunctionArguments {

  def apply(args: Any*): TupleLiteral = TupleLiteral(args.map(_.v).toList)

  def unapply(instruction: Instruction): Option[List[Expression]] = {
    instruction match {
      case ab: ArgumentBlock => Some(ab.args)
      case ee: Expression => Some(List(ee))
      case _ => None
    }
  }

}
