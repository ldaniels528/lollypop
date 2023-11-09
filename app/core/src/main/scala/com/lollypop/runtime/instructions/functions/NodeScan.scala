package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_CONCURRENCY
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

/**
 * NodeScan - Returns an array of Lollypop peer node port numbers.
 */
case class NodeScan() extends ScalarFunctionCall with RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Array[Int]) = {
    (scope, IOCost.empty, LollypopServers.peers.toArray)
  }
}

object NodeScan extends FunctionCallParserE0(
  name = "nodeScan",
  category = CATEGORY_CONCURRENCY,
  description =
    """|Returns an array of Lollypop peer node port numbers.
       |""".stripMargin,
  example = "nodeScan()")

