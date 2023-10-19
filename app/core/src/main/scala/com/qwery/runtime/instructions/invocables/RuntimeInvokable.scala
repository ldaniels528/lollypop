package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Invokable
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.RuntimeInstruction
import qwery.io.IOCost

/**
 * Represents a run-time Invokable
 */
trait RuntimeInvokable extends Invokable with RuntimeInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = invoke()

  def invoke()(implicit scope: Scope): (Scope, IOCost, Any)

}
