package com.qwery.database
package clients

import com.qwery.database.clients.WebServiceClient.QweryResponseConversion
import com.qwery.runtime.ModelsJsonProtocol.IOCostJsonFormat
import qwery.io.IOCost

/**
 * Qwery Message Producer
 * @param host the remote hostname
 * @param port the remote port
 */
case class MessageProducer(host: String = "0.0.0.0", port: Int) {
  private val $http = new WebServiceClient()
  private val charSetName = "utf-8"

  /**
   * Appends a new message to the table
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the table name
   * @param message      the JSON message to append
   * @return the [[IOCost update counts]]
   */
  def send(databaseName: String, schemaName: String, tableName: String, message: String): IOCost = {
    $http.post(toUrl(databaseName, schemaName, tableName), body = message.getBytes(charSetName)).as[IOCost]
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toUrl(databaseName: String, schemaName: String, tableName: String): String = s"http://$host:$port/m/$databaseName/$schemaName/$tableName"

}