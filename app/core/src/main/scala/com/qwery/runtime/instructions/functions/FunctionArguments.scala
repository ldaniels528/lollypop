package com.qwery.runtime.instructions.functions

import com.qwery.language.models.{Expression, Instruction}

/**
 * Utility that provides an <code>unapply</code> for returning function arguments
 */
object FunctionArguments {

  def unapply(instruction: Instruction): Option[List[Expression]] = {
    instruction match {
      case ab: ArgumentBlock => Some(ab.args)
      case ee: Expression => Some(List(ee))
      case _ => None
    }
  }

}
