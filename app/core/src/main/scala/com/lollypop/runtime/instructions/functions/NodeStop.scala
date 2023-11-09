package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_CONCURRENCY
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import lollypop.io.IOCost

case class NodeStop(expression: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    (scope, IOCost.empty, expression.asInt32.exists(LollypopServers.stop))
  }
}

object NodeStop extends FunctionCallParserE1(
  name = "nodeStop",
  category = CATEGORY_CONCURRENCY,
  description =
    """|shuts down a running Lollypop peer node.
       |""".stripMargin,
  example = "nodeStop(8233)")
