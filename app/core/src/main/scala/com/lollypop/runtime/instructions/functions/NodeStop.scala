package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression

case class NodeStop(expression: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = expression.asInt32.exists(LollypopServers.stop)
}

object NodeStop extends FunctionCallParserE1(
  name = "nodeStop",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|shuts down a running Lollypop peer node.
       |""".stripMargin,
  example = "nodeStop(8233)")
