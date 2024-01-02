package com.lollypop.runtime.instructions.conditions

import com.lollypop.runtime.{LollypopVMAddOns, Scope}
import lollypop.io.IOCost

/**
 * Base class for "equals" implementations
 */
trait AbstractEquals extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, va) = a.execute(scope)
    val (sb, cb, vb) = b.execute(sa)
    (sb, ca ++ cb, va == vb)
  }

}
