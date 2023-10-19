package com.qwery.database
package jdbc

import com.qwery.AppConstants._
import com.qwery.database.clients.DatabaseClient
import com.qwery.die

import java.sql.{Connection, Driver, DriverManager, DriverPropertyInfo}
import java.util.Properties
import java.util.logging.Logger
import scala.beans.BeanProperty

/**
 * Qwery JDBC Driver
 */
class QweryDriver extends Driver {
  private val urlPattern = "jdbc:qwery://(\\S+):(\\d+)/(\\S+)".r // (e.g. "jdbc:qwery://localhost:8233/portfolios.stocks")

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
      new DriverPropertyInfo("port", DEFAULT_PORT)
    )
  }

  override def jdbcCompliant(): Boolean = false

}

/**
 * Qwery JDBC Driver Companion
 */
object QweryDriver {

  // register the driver
  DriverManager.registerDriver(new QweryDriver())

}