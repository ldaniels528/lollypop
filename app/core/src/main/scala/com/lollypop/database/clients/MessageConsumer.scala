package com.lollypop.database.clients

import com.lollypop.language.dieIllegalType
import com.lollypop.runtime._
import spray.json.JsObject

import scala.util.{Failure, Success, Try}

/**
 * Lollypop Message Consumer
 * @param host the remote hostname
 * @param port the remote port
 * @param ns   the [[DatabaseObjectNS table reference]]
 */
case class MessageConsumer(host: String = "0.0.0.0", port: Int, ns: DatabaseObjectNS) {
  private val $http = new WebServiceClient()
  private var rowID: ROWID = 0

  def getMessage(offset: ROWID): Option[Map[String, Any]] = {
    $http.get(toUrl(ns.name, offset)) match {
      case js: JsObject => Option(js.fields.map { case (name, jv) => name -> jv.unwrapJSON })
      case js => dieIllegalType(js)
    }
  }

  def getNextMessage: Option[Map[String, Any]] = {
    Try(getMessage(rowID)) match {
      case Success(value) =>
        rowID += 1
        value
      case Failure(e) =>
        e.printStackTrace()
        None
    }
  }

  def seek(offset: ROWID): Unit = {
    assert(offset >= 0, "Negative offsets are not supported")
    rowID = offset
  }

  //////////////////////////////////////////////////////////////////////
  //      URL GENERATORS
  //////////////////////////////////////////////////////////////////////

  private def toUrl(tableName: String, rowID: ROWID): String = s"http://$host:$port/m/${ns.databaseName}/${ns.schemaName}/${ns.name}/$rowID"

}