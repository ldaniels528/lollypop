package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.qwery.language.models.Expression
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.conditions.RuntimeCondition
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression

case class NodeStop(expression: Expression) extends ScalarFunctionCall with RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = expression.asInt32.exists(QweryServers.stop)
}

object NodeStop extends FunctionCallParserE1(
  name = "nodeStop",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|shuts down a running Qwery peer node.
       |""".stripMargin,
  example = "nodeStop(8233)")
