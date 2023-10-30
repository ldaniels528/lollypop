package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression

case class NodeScan() extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Int] = LollypopServers.peers.toArray

}

object NodeScan extends FunctionCallParserE0(
  name = "nodeScan",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|Returns an array of Lollypop peer node port numbers.
       |""".stripMargin,
  example = "nodeScan()")

