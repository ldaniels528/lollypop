package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.lollypop.language.models.Expression
import com.lollypop.runtime.datatypes.Int32Type
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost
import org.slf4j.LoggerFactory

/**
 * NodeConsole - Opens a commandline interface to a remote Lollypop peer node.
 * @example {{{
 *    set port = nodeStart()
 *    nodeConsole(port)
 * }}}
 * @param portExpr     the port [[Expression expression]]
 * @param commandsExpr the commands [[Expression expression]]
 */
case class NodeConsole(portExpr: Expression, commandsExpr: Option[Expression])
  extends ScalarFunctionCall with RuntimeExpression {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (_, cost1, result1) = LollypopVM.execute(scope, portExpr)
    val port = Int32Type.convert(result1)
    val commands_? = for {
      commandsE <- commandsExpr
      array = LollypopVM.execute(scope, commandsE)._3 match {
        case array: Array[String] => array
        case other => commandsE.dieIllegalType(other)
      }
    } yield array

    logger.info(s"Connecting to remote peer on port $port...")
    val result = commands_? match {
      case Some(commands) =>
        LollypopServers.evaluate(port, commands.mkString("\n"), scope)
      case None =>
        LollypopServers.cli(port)
        null
    }
    (scope, cost1, result)
  }

}

object NodeConsole extends FunctionCallParserE1Or2(
  name = "nodeConsole",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|Opens a commandline interface to a remote Lollypop peer node.
       |""".stripMargin,
  example =
    """|val remotePort = nodeStart()
       |after Interval("5 seconds") nodeStop(remotePort)
       |nodeConsole(remotePort, [
       |  "from help() limit 6"
       |])
       |""".stripMargin)

