package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.qwery.language.models.Expression
import com.qwery.runtime.datatypes.Int32Type
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.{QweryVM, Scope}
import org.slf4j.LoggerFactory

/**
 * NodeConsole - Opens a commandline interface to a remote Qwery peer node.
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

  override def evaluate()(implicit scope: Scope): Any = {
    val (_, _, result1) = QweryVM.execute(scope, portExpr)
    val port = Int32Type.convert(result1)
    val commands_? = for {
      commandsE <- commandsExpr
      array = QweryVM.execute(scope, commandsE)._3 match {
        case array: Array[String] => array
        case other => commandsE.dieIllegalType(other)
      }
    } yield array

    logger.info(s"Connecting to remote peer on port $port...")
    commands_? match {
      case Some(commands) =>
        QweryServers.evaluate(port, commands.mkString("\n"), scope)
      case None =>
        QweryServers.cli(port)
        null
    }
  }
}

object NodeConsole extends FunctionCallParserE1Or2(
  name = "nodeConsole",
  category = CATEGORY_ASYNC_REACTIVE,
  description =
    """|Opens a commandline interface to a remote Qwery peer node.
       |""".stripMargin,
  example =
    """|val remotePort = nodeStart()
       |after Interval("5 seconds") nodeStop(remotePort)
       |nodeConsole(remotePort, [
       |  "from help() limit 6"
       |])
       |""".stripMargin)

