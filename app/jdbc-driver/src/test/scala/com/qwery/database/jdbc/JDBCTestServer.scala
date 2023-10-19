package com.qwery.database.jdbc

import com.qwery.database.server.QweryServers
import org.slf4j.{Logger, LoggerFactory}

trait JDBCTestServer {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  // load the JDBC driver
  Class.forName(QweryDriver.getClass.getName)

  // start the server
  val port: Int = QweryServers.start()

  val jdbcURL: String = {
    val (databaseName, schemaName, _) = getTestTableDetails
    s"jdbc:qwery://localhost:$port/$databaseName.$schemaName"
  }

  def getTestTableDetails: (String, String, String) = {
    val pcs = this.getClass.getName.split("[.]")
    "test" :: pcs.takeRight(2).toList match {
      case List(databaseName, schemaName, name) => (databaseName, schemaName, name)
    }
  }

}
