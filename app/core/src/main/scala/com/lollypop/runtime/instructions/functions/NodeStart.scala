package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.{CATEGORY_CONCURRENCY, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.instructions.expressions.{IntegerExpression, RuntimeExpression}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import scala.concurrent.duration.Duration

case class NodeStart(port: Option[Expression]) extends ScalarFunctionCall with RuntimeExpression with IntegerExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Int) = {
    val result = port match {
      case Some(expression) =>
        val _port = expression.asInt32 || expression.die("Integer value expected")
        LollypopServers.startManually(_port)(scope.getUniverse, Duration.Inf)
        _port
      case None => LollypopServers.start(scope.getUniverse)
    }
    (scope, IOCost.empty, result)
  }

}

object NodeStart extends FunctionCallParserE0Or1(
  name = "nodeStart",
  category = CATEGORY_CONCURRENCY,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Starts a Lollypop peer node.
       |""".stripMargin,
  example = "nodeStart()")
