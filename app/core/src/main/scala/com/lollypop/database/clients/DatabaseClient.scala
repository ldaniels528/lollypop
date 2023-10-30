package com.lollypop.database
package clients

import com.lollypop.database.clients.WebServiceClient._
import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.dieExpectedJSONObject
import com.lollypop.runtime.ModelsJsonProtocol._
import com.lollypop.runtime.ROWID
import com.lollypop.runtime.devices.Row
import com.lollypop.util.JSONSupport.JsValueConversion
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.{IOCost, RowIDRange}
import spray.json._

import java.io.File

/**
 * Lollypop Database Client
 * @param host the remote hostname
 * @param port the remote port
 */
case class DatabaseClient(host: String = "0.0.0.0", port: Int) {
  private val charSetName = "utf-8"
  private val $http = new WebServiceClient()
  private var closed = false

  /**
   * Closes the connection to the server
   */
  def close(): Unit = closed = true

  /**
   * Indicates whether the connection to the server has been closed
   * @return true, if [[close]] has previously been called
   */
  def isClosed: Boolean = closed

  /**
   * Deletes the contents of a field; rending it null.
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the table name
   * @param rowID        the row ID of the field
   * @param columnID     the column ID of the field
   * @return the [[IOCost I/O cost]]
   */
  def deleteField(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): IOCost = {
    $http.delete(url = s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnID").as[IOCost]
  }

  /**
   * Deletes a row by ID
   * @param databaseName the name of the database
   * @param schemaName   the name of the schema
   * @param tableName    the table name
   * @param rowID        the ID of the row to delete
   * @return the [[IOCost I/O cost]]
   */
  def deleteRow(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): IOCost = {
    $http.delete(toUrl(databaseName, schemaName, tableName, rowID)).as[IOCost]
  }

  /**
   * Executes a SQL statement or query
   * @param databaseName the default database
   * @param schemaName   the default schema
   * @param sql          the SQL statement or query
   * @param limit        the optional results limit
   * @return the [[QueryResponse]]
   */
  def executeQuery(databaseName: String, schemaName: String, sql: String, limit: Option[Int] = None): QueryResponse = {
    $http.post(toQueryUrl(databaseName, schemaName, limit), body = sql.getBytes(charSetName)).convertTo[QueryResponse]
  }

  /**
   * Executes a SQL statement or query
   * @param databaseName the default database
   * @param schemaName   the default schema
   * @param request      the [[QueryRequest JDBC request]]
   * @return the [[QueryResponse]]
   */
  def executeJDBC(databaseName: String, schemaName: String, request: QueryRequest): QueryResponse = {
    $http.post(toJDBCUrl(databaseName, schemaName), body = request.toJson.compactPrint.getBytes(charSetName)).convertTo[QueryResponse]
  }

  def getFieldAsBytes(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): Array[Byte] = {
    $http.getAsBytes(url = s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnID")
  }

  def getFieldAsBytes(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnName: String): Array[Byte] = {
    $http.getAsBytes(url = s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnName")
  }

  def getFieldAsFile(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): File = {
    $http.getAsFile(url = s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnID")
  }

  def getFieldAsFile(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnName: String): File = {
    $http.getAsFile(url = s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnName")
  }

  def getLength(databaseName: String, schemaName: String, tableName: String): Long = {
    $http.get(url = s"${toUrl(databaseName, schemaName, tableName)}/length").as[Long]
  }

  def setLength(databaseName: String, schemaName: String, tableName: String, newSize: Long): IOCost = {
    $http.post(url = s"${toUrl(databaseName, schemaName, tableName)}/length?newSize=$newSize").as[IOCost]
  }

  def getRow(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): Option[Map[String, Any]] = {
    $http.get(toUrl(databaseName, schemaName, tableName, rowID)) match {
      case js: JsObject => Option(js.fields.map { case (name, jv) => name -> jv.unwrapJSON })
      case js => dieExpectedJSONObject(js)
    }
  }

  def getRowWithMetadata(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): Row = {
    $http.get(toUrl(databaseName, schemaName, tableName, rowID)).convertTo[Row]
  }

  def getTableMetrics(databaseName: String, schemaName: String, tableName: String): DatabaseEntityMetrics = {
    $http.get(toUrl(databaseName, schemaName, tableName)).as[DatabaseEntityMetrics]
  }

  def insertRow(databaseName: String, schemaName: String, tableName: String, values: Map[String, Any]): IOCost = {
    $http.post(toUrl(databaseName, schemaName, tableName), values.toJson.toString().getBytes(charSetName)).as[IOCost]
  }

  def replaceRow(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, values: Map[String, Any]): IOCost = {
    $http.put(toUrl(databaseName, schemaName, tableName, rowID), values.toJson.toString().getBytes(charSetName))
    IOCost(updated = 1, rowIDs = RowIDRange(rowID))
  }

  def updateField(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int, value: Option[Any]): IOCost = {
    $http.put(toUrl(databaseName, schemaName, tableName, rowID, columnID), value.toJson.toString().getBytes(charSetName))
    IOCost(updated = 1, rowIDs = RowIDRange(rowID))
  }

  def updateRow(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, values: Map[String, Any]): IOCost = {
    $http.post(toUrl(databaseName, schemaName, tableName, rowID), values.toJson.toString().getBytes(charSetName)).as[IOCost]
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toJDBCUrl(databaseName: String, schemaName: String): String = {
    s"http://$host:$port/jdbc/$databaseName/$schemaName"
  }

  private def toQueryUrl(databaseName: String, schemaName: String, limit: Option[Int]): String = {
    s"http://$host:$port/q/$databaseName/$schemaName" ~> { url => limit.map(n => s"$url?__limit=$n") || url }
  }

  private def toUrl(databaseName: String, schemaName: String, tableName: String): String = {
    s"http://$host:$port/d/$databaseName/$schemaName/$tableName"
  }

  private def toUrl(databaseName: String, schemaName: String, tableName: String, rowID: ROWID): String = {
    s"${toUrl(databaseName, schemaName, tableName)}/$rowID"
  }

  private def toUrl(databaseName: String, schemaName: String, tableName: String, rowID: ROWID, columnID: Int): String = {
    s"${toUrl(databaseName, schemaName, tableName)}/$rowID/$columnID"
  }

}