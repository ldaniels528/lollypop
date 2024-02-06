package com.lollypop.database.jdbc

import com.lollypop.runtime.instructions.VerificationTools.closeOnShutdown
import lollypop.io.{Node, Nodes}
import org.slf4j.{Logger, LoggerFactory}

trait JDBCTestServer {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  // load the JDBC driver
  Class.forName(LollypopDriver.getClass.getName)

  // start the server
  val node: Node = Nodes().start()
  val port: Int = node.server.port
  val jdbcURL: String = {
    val (databaseName, schemaName, _) = getTestTableDetails
    s"jdbc:lollypop://localhost:$port/$databaseName.$schemaName"
  }

  closeOnShutdown(node)

  def getTestTableDetails: (String, String, String) = {
    val pcs = this.getClass.getName.split("[.]")
    "test" :: pcs.takeRight(2).toList match {
      case List(databaseName, schemaName, name) => (databaseName, schemaName, name)
    }
  }

}
