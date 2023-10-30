package com.lollypop.runtime.instructions.functions

import com.lollypop.database.server.LollypopServers
import com.lollypop.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.lollypop.language.models.Expression
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression

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

  override def isTrue(implicit scope: Scope): Boolean = {
    (for {
      myPort <- port.asInt32
      myUrl <- url.asString
      myFiles <- files.asDictionaryOf[String]
      ok <- LollypopServers.createFileEndPoint(myPort, myUrl, myFiles.toMap)
    } yield ok) contains true
  }
}

object NodeWWW extends FunctionCallParserE3(
  name = "nodeWWW",
  category = CATEGORY_ASYNC_REACTIVE,
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
