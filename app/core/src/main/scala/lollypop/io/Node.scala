package lollypop.io

import akka.http.scaladsl.Http
import com.lollypop.AppConstants.{DEFAULT_DATABASE, DEFAULT_SCHEMA}
import com.lollypop.database.QueryResponse
import com.lollypop.database.clients.DatabaseClient
import com.lollypop.database.server.LollypopServer
import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.QMap
import com.lollypop.util.ConsoleReaderHelper.{createInteractiveConsoleReader, interactWith}
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper.time
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Represents a Lollypop server node
 */
case class Node(ctx: LollypopUniverse, host: String = "0.0.0.0", port: Int, server: LollypopServer) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val client = DatabaseClient(host, port)
  private val scope: Scope = ctx.createRootScope()
  private val startTime = System.currentTimeMillis()
  var lastCommand: Option[String] = None

  /**
   * Creates a new REST API endpoint
   * @param url     the URL string
   * @param methods the HTTP method and code mapping
   * @example {{{
   *  node.api('/api/subscriptions/', {
   *    post: createSubscription,
   *    get: readSubscription,
   *    put: updateSubscription,
   *    delete: deleteSubscription
   *    ws: handleWebSocket
   *  })
   * }}}
   */
  def api(url: String, methods: QMap[String, Any]): Boolean = {
    server.createAPIEndPoint(url, methods.toMap)
  }

  def awaitStartup(timeout: Duration): Node = {
    val (_, millis) = time(Await.ready(getServerBinding, timeout))
    logger.info(f"Node($port) started in $millis%.1f msec")
    this
  }

  def getServerBinding: Future[Http.ServerBinding] = server.server

  def interact(console: () => String = createInteractiveConsoleReader): Option[QueryResponse] = {
    var response_? : Option[QueryResponse] = None
    interactWith(
      console = console,
      executeCode = { (d, s, q) =>
        lastCommand = Some(q)
        Try(client.executeQuery(d, s, q)) map { response =>
          response_? = Option(response)
          (Some(response.ns.databaseName), Some(response.ns.schemaName))
        }
      })
    response_?
  }

  def exec(statement: String): QueryResponse = {
    val databaseName = scope.getDatabase || DEFAULT_DATABASE
    val schemaName = scope.getSchema || DEFAULT_SCHEMA
    lastCommand = Some(statement)
    client.executeQuery(databaseName, schemaName, statement)
  }

  def exec(statements: Array[String]): QueryResponse = {
    exec(statements.mkString(";"))
  }

  def stop(): Unit = server.shutdown()

  def uptime: Long = System.currentTimeMillis() - startTime

  /**
   * Creates a new HTML/CSS/File endpoint
   * @param files the HTTP URL to file mapping
   * @return true, if a new endpoint was created
   */
  def www(url: String, files: Map[String, String]): Boolean = server.createFileEndPoint(url, files)

  override def toString: String = s"Node(\"$host\", $port)"

}