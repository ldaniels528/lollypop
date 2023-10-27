package com.qwery.runtime.instructions.functions

import com.qwery.database.server.QweryServers
import com.qwery.language.HelpDoc.CATEGORY_ASYNC_REACTIVE
import com.qwery.language.models.Expression
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.conditions.RuntimeCondition
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression

/**
 * Creates a new REST API endpoint
 * @param port the port for which to bind the service
 * @example {{{
 *  nodeAPI(port, '/api/subscriptions/', {
 *    post: createSubscription,
 *    get: readSubscription,
 *    put: updateSubscription,
 *    delete: deleteSubscription
 *    ws: handleWebSocket
 *  })
 * }}}
 */
case class NodeAPI(port: Expression, url: Expression, methods: Expression)
  extends ScalarFunctionCall with RuntimeCondition {

  override def isTrue(implicit scope: Scope): Boolean = {
    (for {
      myPort <- port.asInt32
      myUrl <- url.asString
      myMethods <- methods.asDictionary
      ok <- QweryServers.createAPIEndPoint(myPort, myUrl, myMethods.toMap)
    } yield ok) contains true
  }
}

object NodeAPI extends FunctionCallParserE3(
  name = "nodeAPI",
  category = CATEGORY_ASYNC_REACTIVE,
  description = "Creates a new REST API endpoint",
  example =
    """|import "java.lang.Thread"
       |var port = nodeStart()
       |nodeAPI(port, '/api/comments/', {
       |  post: (message: String) => { out <=== "post '{{message}}'" },
       |  get: (id: UUID) => { out <=== "get {{(id}}" },
       |  put: (id: UUID, message: String) => { out <=== "put '{{message}}' ~> {{(id}}" },
       |  delete: (id: UUID) => { out <=== "delete {{(id}}" }
       |})
       |Thread.sleep(Long(100))
       |http post "http://0.0.0.0:{{port}}/api/comments/" <~ { message: "Hello World" }
       |""".stripMargin)

