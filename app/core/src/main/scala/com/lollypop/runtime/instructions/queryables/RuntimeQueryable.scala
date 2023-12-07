package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Queryable
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.RuntimeInstruction
import lollypop.io.IOCost

import scala.language.postfixOps

/**
 * Represents a run-time Queryable
 */
trait RuntimeQueryable extends Queryable with RuntimeInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection)

}
