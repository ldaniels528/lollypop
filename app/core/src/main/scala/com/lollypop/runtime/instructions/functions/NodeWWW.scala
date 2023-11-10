package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_CONCURRENCY
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import lollypop.io.IOCost

/**
 * Creates a new HTML/CSS/File endpoint
 * @param port the port for which to bind the service
 * @example {{{
 *  nodeWWW(8135, '/www/notebooks/', {
 *    "" : "public/index.html",
 *    "*" : "public"
 *  })
 * }}}
 */
case class NodeWWW(port: Expression, url: Expression, files: Expression)
  extends ScalarFunctionCall with RuntimeCondition {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val result = (for {
      myPort <- port.asInt32
      myUrl <- url.asString
      myFiles <- files.asDictionaryOf[String]
    } yield LollypopServers.www(myPort, myUrl, myFiles.toMap)) contains true
    (scope, IOCost.empty, result)
  }
}

object NodeWWW extends FunctionCallParserE3(
  name = "nodeWWW",
  category = CATEGORY_CONCURRENCY,
  description = "Creates a new HTML/CSS/File endpoint",
  example =
    """|import "java.lang.Thread"
       |
       |val port = nodeStart()
       |Thread.sleep(Long(1000))
       |nodeWWW(port, '/www/notebooks/', {
       |  "" : "public/index.html",
       |  "*" : "public"
       |})
       |""".stripMargin)
