package com.lollypop.database
package jdbc

import com.lollypop.database.clients.DatabaseClient
import com.lollypop.die
import com.lollypop.runtime._

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger
import scala.beans.BeanProperty

/**
 * Lollypop JDBC Driver
 */
class LollypopDriver extends Driver {
  private val urlPattern = "jdbc:lollypop://(\\S+):(\\d+)/(\\S+)".r // (e.g. "jdbc:lollypop://localhost:8233/portfolios.stocks")

  @BeanProperty val majorVersion: Int = 0
  @BeanProperty val minorVersion: Int = 5
  @BeanProperty val parentLogger: Logger = Logger.getLogger(getClass.getName)

  override def acceptsURL(url: String): Boolean = urlPattern.findAllIn(url).nonEmpty

  override def connect(url: String, info: Properties): Connection = {
    url match {
      case urlPattern(host, port, path) =>
        // extract the database name and schema name from the path
        val (databaseName, schemaName) = path.split('.') match {
          case Array(databaseName, schemaName) => (databaseName, schemaName)
          case Array(databaseName) => (databaseName, DEFAULT_SCHEMA)
          case _ => die(s"Invalid path '$path': Expected format is 'databaseName[.schemaName]' (e.g. 'myApp.dev' or 'test')")
        }
        // create the connection
        val conn = new JDBCConnection(client = DatabaseClient(host, port.toInt), url = url)
        conn.setCatalog(databaseName)
        conn.setSchema(schemaName)
        conn
      case x => die(s"Invalid JDBC URL: $x")
    }
  }

  override def getPropertyInfo(url: String, info: Properties): Array[DriverPropertyInfo] = {
    Array(
      new DriverPropertyInfo("database", DEFAULT_DATABASE),
      new DriverPropertyInfo("schema", DEFAULT_SCHEMA),
      new DriverPropertyInfo("server", DEFAULT_HOST),
      new DriverPropertyInfo("port", DEFAULT_PORT.toString)
    )
  }

  override def jdbcCompliant(): Boolean = false

}

/**
 * Lollypop JDBC Driver Companion
 */
object LollypopDriver {

  // register the driver
  DriverManager.registerDriver(new LollypopDriver())

}