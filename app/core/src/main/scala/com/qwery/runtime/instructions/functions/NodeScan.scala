package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression

case class NodeScan() extends ScalarFunctionCall with RuntimeExpression {
  override def evaluate()(implicit scope: Scope): Array[Int] = QweryServers.peers.toArray

}

object NodeScan extends FunctionCallParserE0(
  name = "nodeScan",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|Returns an array of Qwery peer node port numbers.
       |""".stripMargin,
  example = "nodeScan()")

