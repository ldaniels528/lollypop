package lollypop.io

import akka.http.scaladsl.Http
import com.lollypop.database.QueryResponse
import com.lollypop.database.clients.DatabaseClient
import com.lollypop.database.server.LollypopServer
import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.devices.QMap
import com.lollypop.util.ConsoleReaderHelper.{createInteractiveConsoleReader, interactWith}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * Represents a Lollypop server node
 * @param server the provided [[LollypopServer]]
 */
case class Node(server: LollypopServer) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val client = DatabaseClient(server.host, server.port)
  private val scope: Scope = server.ctx.createRootScope()
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
  def api(url: String, methods: QMap[String, Any]): Boolean = server.createAPIEndPoint(url, methods.toMap)

  def awaitStartup(timeout: Duration): Node = {
    val (_, millis) = time(Await.ready(getServerBinding, timeout))
    logger.info(f"Node(${server.port}) started in $millis%.1f msec")
    this
  }

  /**
   * Creates a new HTML/CSS/File endpoint
   * @param urlMappings the HTTP URL to file mapping
   * @return true, if a new endpoint was created
   */
  def files(url: String, urlMappings: QMap[String, String]): Boolean = server.createFileEndPoint(url, urlMappings)

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

  def exec(statements: Array[String]): QueryResponse = exec(statements.mkString(";"))

  val host: String = server.host

  val port: Int = server.port

  def stop(): Unit = server.shutdown()

  def uptime: Long = System.currentTimeMillis() - startTime

  override def toString: String = s"Node(\"${server.host}\", ${server.port})"

}