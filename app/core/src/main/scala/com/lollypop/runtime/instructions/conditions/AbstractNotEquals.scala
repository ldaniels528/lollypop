package com.lollypop.runtime.instructions.conditions

import com.lollypop.runtime._
import lollypop.io.IOCost

/**
 * Base class for "not equal" implementations
 */
trait AbstractNotEquals extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, va) = a.execute(scope)
    val (sb, cb, vb) = b.execute(sa)
    (sb, ca ++ cb, va != vb)
  }

}
