package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.{IntegerExpression, RuntimeExpression}
import com.lollypop.util.OptionHelper.OptionEnrichment

import scala.concurrent.duration.Duration

case class NodeStart(port: Option[Expression]) extends ScalarFunctionCall with RuntimeExpression with IntegerExpression {
  override def evaluate()(implicit scope: Scope): Int = {
    port match {
      case Some(expression) =>
        val _port = expression.asInt32 || expression.die("Integer value expected")
        LollypopServers.startManually(_port)(scope.getUniverse, Duration.Inf)
        _port
      case None => LollypopServers.start(scope.getUniverse)
    }
  }

}

object NodeStart extends FunctionCallParserE0Or1(
  name = "nodeStart",
  category = CATEGORY_ASYNC_REACTIVE,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Starts a Lollypop peer node.
       |""".stripMargin,
  example = "nodeStart()")
