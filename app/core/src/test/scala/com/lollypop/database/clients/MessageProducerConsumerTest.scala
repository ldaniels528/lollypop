package com.lollypop.database
package clients

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.VerificationTools.closeOnShutdown
import com.lollypop.runtime.{DatabaseObjectNS, DatabaseObjectRef}
import lollypop.io.Nodes
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Messaging Producer-Consumer Test Suite
 * @author lawrence.daniels@gmail.com
 */
class MessageProducerConsumerTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val node = Nodes().start()
  private val port = node.port
  val Array(databaseName, schemaName, tableName) = getTestTableName.split("[.]")
  private val table = DatabaseObjectRef(tableName)

  // create the clients
  private val databaseClient = DatabaseClient(port = port)
  private val messageProducer = MessageProducer(port = port)
  private val messageConsumer = MessageConsumer(port = port, ns = DatabaseObjectNS(databaseName, schemaName, tableName))

  closeOnShutdown(node)

  describe(classOf[DatabaseClient].getSimpleName) {

    it("should create a new table on the server") {
      val response = databaseClient.executeQuery(databaseName, schemaName,
        s"""|drop if exists $table
            |create table $table (
            |  symbol: String(8),
            |  exchange: String(8),
            |  lastSale: Double,
            |  lastSaleTime: Long
            |)
            |""".stripMargin, limit = None)
      response.tabulate().foreach(logger.info)
      assert(response.getUpdateCount == 1)
    }
  }

  describe(classOf[MessageProducer].getSimpleName) {

    it("should append messages to a table on the server") {
      val message = """{"symbol":"AAPL", "exchange":"NYSE", "lastSale":900, "lastSaleTime":1611772605427}"""
      logger.info(s"message: $message")

      val response = messageProducer.send(databaseName, schemaName, tableName, message)
      logger.info(s"response = $response")
      assert(response.inserted == 1)
    }
  }

  describe(classOf[DatabaseClient].getSimpleName) {

    it("should query messages from the server") {
      val results = databaseClient.executeQuery(databaseName, schemaName, s"select * from $table", limit = None)
      results.foreach { row =>
        logger.info(s"row: $row")
        assert(row == Map("exchange" -> "NYSE", "symbol" -> "AAPL", "lastSale" -> 900.0, "lastSaleTime" -> 1611772605427L))
      }
      assert(results.rows.size == 1)
    }
  }

  describe(classOf[MessageConsumer].getSimpleName) {

    it("should retrieve messages (if available) from the server") {
      val message = messageConsumer.getNextMessage
      logger.info(s"message: $message")
      assert(message contains Map("exchange" -> "NYSE", "symbol" -> "AAPL", "lastSale" -> 900.0, "lastSaleTime" -> 1611772605427L))
    }
  }

}