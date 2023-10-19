package com.qwery.database.server

import akka.http.scaladsl.Http
import com.qwery.AppConstants._
import com.qwery.database.QueryResponse
import com.qwery.database.clients.DatabaseClient
import com.qwery.language.QweryUniverse
import com.qwery.runtime.QweryCodeDebugger.createInteractiveConsoleReader
import com.qwery.runtime.{DatabaseObjectNS, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import com.qwery.util.ResourceHelper.time
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util._

/**
 * Qwery Server Manager
 */
object QweryServers {
  private val logger = LoggerFactory.getLogger(getClass)
  private val servers = TrieMap[Int, QweryServer]()

  def cli(port: Int): Unit = {
    val client = DatabaseClient(port = port)
    interact(port)(client)
  }

  def createAPIEndPoint(port: Int, url: String, methods: Map[String, Any]): Option[Boolean] = {
    servers.get(port).map(_.createAPIEndPoint(url, methods))
  }

  def createFileEndPoint(port: Int, url: String, files: Map[String, String]): Option[Boolean] = {
    servers.get(port).map(_.createFileEndPoint(url, files))
  }

  def evaluate(port: Int, sql: String, scope: Scope): QueryResponse = {
    val client = DatabaseClient(port = port)
    val database = scope.getDatabase || DEFAULT_DATABASE
    val schema = scope.getSchema || DEFAULT_SCHEMA
    client.executeQuery(database, schema, sql)
  }

  def getServerBinding(port: Int): Option[Future[Http.ServerBinding]] = {
    servers.get(port).map(_.server)
  }

  def peers: List[Int] = servers.toList.map(_._1)

  def start(ctx: QweryUniverse = QweryUniverse(isServerMode = true), timeout: Duration = Duration.Inf): Int = {
    val random = new Random(System.currentTimeMillis())

    @tailrec
    def startUpOnRandomPort(): Int = {
      val port = random.nextInt(8000) + 8000
      if (servers.contains(port)) startUpOnRandomPort() else {
        startManually(port)(ctx, timeout)
        port
      }
    }

    startUpOnRandomPort()
  }

  def startManually(port: Int)(implicit ctx: QweryUniverse, timeout: Duration): QweryServer = {
    val server = servers.getOrElseUpdate(port, QweryServer(port, ctx))
    AwaitStartup(port, timeout)
    server
  }

  def stop(port: Int): Boolean = {
    servers.remove(port).exists { server =>
      server.shutdown()
      true
    }
  }

  private def AwaitStartup(port: Int, timeout: Duration): Unit = {
    logger.info(s"Awaiting server($port) startup...")
    val (_, msec) = time(getServerBinding(port).foreach { binding =>
      Await.ready(binding, timeout)
    })
    logger.info(f"Server($port) started in $msec%.1f msec")
  }

  private def interact(port: Int, console: () => String = createInteractiveConsoleReader)(implicit client: DatabaseClient): Unit = {
    import scala.Console._
    var isDone: Boolean = false
    var databaseName = DEFAULT_DATABASE
    var schemaName = DEFAULT_SCHEMA

    try {
      do {
        // hide the prompt if in a multi-line sequence
        Console.print(s"$RESET$CYAN${Properties.userName}:$MAGENTA$port$CYAN/$databaseName/$schemaName$RESET> ")
        val input = console().trim
        if (input.nonEmpty) Console.println(s"$RESET${YELLOW}Processing request... (Sent ${input.length} bytes)$RESET")
        input match {
          // blank line?
          case sql if sql.isEmpty => ()
          // quit the CLI
          case sql if sql.toLowerCase == "exit" | sql.toLowerCase == "quit" => isDone = true
          // execute a complete statement?
          case sql =>
            executeQuery(databaseName, schemaName, sql) foreach { ns =>
              databaseName = ns.databaseName
              schemaName = ns.schemaName
            }
        }
      } while (!isDone)
    } finally client.close()
  }

  private def executeQuery(databaseName: String, schemaName: String, sql: String)(implicit client: DatabaseClient): Option[DatabaseObjectNS] = {
    Try(client.executeQuery(databaseName, schemaName, sql)) match {
      case Success(queryResult) =>
        queryResult.tabulate() foreach Console.println
        Some(queryResult.ns)
      case Failure(e) =>
        Console.err.println(e.getMessage)
        Console.println()
        None
    }
  }

}
