package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.{CATEGORY_DISTRIBUTED, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.Expression
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.expressions.{IntegerExpression, RuntimeExpression}
import com.qwery.util.OptionHelper.OptionEnrichment

import scala.concurrent.duration.Duration

case class NodeStart(port: Option[Expression]) extends ScalarFunctionCall with RuntimeExpression with IntegerExpression {
  override def evaluate()(implicit scope: Scope): Int = {
    port match {
      case Some(expression) =>
        val _port = expression.asInt32 || expression.die("Integer value expected")
        QweryServers.startManually(_port)(scope.getUniverse, Duration.Inf)
        _port
      case None => QweryServers.start(scope.getUniverse)
    }
  }

}

object NodeStart extends FunctionCallParserE0Or1(
  name = "nodeStart",
  category = CATEGORY_DISTRIBUTED,
  paradigm = PARADIGM_FUNCTIONAL,
  description =
    """|Starts a Qwery peer node.
       |""".stripMargin,
  example = "nodeStart()")
