package com.lollypop.database.clients

import com.lollypop.database.QueryResponse
import com.lollypop.language._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.VerificationTools.closeOnShutdown
import com.lollypop.runtime.time
import lollypop.io.Nodes
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Database Client Test Suite
 */
class DatabaseClientTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val node = Nodes().start()
  private val port = node.port

  closeOnShutdown(node)

  describe(classOf[DatabaseClient].getSimpleName) {
    // create the client
    implicit val service: DatabaseClient = DatabaseClient(port = port)
    val Array(databaseName, schemaName, tableNameA) = getTestTableName.split("[.]")

    it("should report the status of the connection") {
      assert(!service.isClosed)
    }

    it("should execute a simple SQL query") {
      val sql =
        s"""|select value: null
            |""".stripMargin.trim
      invoke(sql, service.executeQuery(databaseName, schemaName, sql, limit = None))
    }

    it("should create a new table (SQL)") {
      val sql =
        s"""|namespace '$databaseName.$schemaName'
            |drop if exists $tableNameA
            |create table `$tableNameA` (
            |  symbol: String(8),
            |  exchange: String(8),
            |  lastSale: Double,
            |  lastSaleTime: DateTime
            |)
            |""".stripMargin.trim
      invoke(sql, service.executeQuery(databaseName, schemaName, sql, limit = None))
    }

    it("should append a record to the end of a table") {
      val record = Map("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.insertRow("$tableNameA", $record)""", service.insertRow(databaseName, schemaName, tableNameA, record))
    }

    it("should replace a record at a specific index") {
      val record = Map("symbol" -> "MSFT", "exchange" -> "NYSE", "lastSale" -> 123.55, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.replaceRow("$tableNameA", rowID = 1, $record)""", service.replaceRow(databaseName, schemaName, tableNameA, rowID = 1, values = record))
    }

    it("should update a record at a specific index") {
      val record = Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 56.78, "lastSaleTime" -> System.currentTimeMillis())
      invoke(label = s"""service.updateRow("$tableNameA", rowID = 1, $record)""", service.updateRow(databaseName, schemaName, tableNameA, rowID = 1, values = record))
    }

    it("should append a record to the end of a table (SQL)") {
      val sql =
        s"""|insert into $tableNameA (symbol, exchange, lastSale, lastSaleTime)
            |values ("MSFT", "NYSE", 123.55, new `java.util.Date`())
            |""".stripMargin.singleLine
      invoke(sql, service.executeQuery(databaseName, schemaName, sql, limit = None))
    }

    it("should retrieve a field by ID from a row on the server") {
      val (rowID, columnID) = (0, 0)
      invoke(
        label = s"""service.getFieldAsBytes("$tableNameA", rowID = $rowID, columnID = $columnID)""",
        block = service.getFieldAsBytes(databaseName, schemaName, tableNameA, rowID, columnID)
      )
    }

    it("should retrieve a field by name from a row on the server") {
      val (rowID, columnName) = (0, "symbol")
      invoke(
        label = s"""service.getFieldAsBytes("$tableNameA", rowID = $rowID, columnName = $columnName)""",
        block = service.getFieldAsBytes(databaseName, schemaName, tableNameA, rowID, columnName)
      )
    }

    it("should retrieve table metrics for a table from the server") {
      invoke(s"""service.getTableMetrics("$tableNameA")""", service.getTableMetrics(databaseName, schemaName, tableNameA))
    }

    it("should retrieve a row by ID from the server") {
      invoke(s"""service.getRow("$tableNameA", rowID = 0)""", service.getRow(databaseName, schemaName, tableNameA, rowID = 0))
    }

    it("should execute queries against the server") {
      val sql = s"select * from $tableNameA where exchange == 'NASDAQ' limit 5"
      invoke(sql, service.executeQuery(databaseName, schemaName, sql, limit = None))
    }

    it("should delete a row by ID from the server") {
      invoke(
        label = s"""service.deleteRow("$tableNameA", rowID = 999)""",
        block = service.deleteRow(databaseName, schemaName, tableNameA, rowID = 999))
    }

  }

  def invoke[A](label: String, block: => A): A = {
    val (results, responseTime) = time(block)
    results match {
      case bytes: Array[Byte] =>
        logger.info(f"$label ~> (hex) ${toHexString(bytes)} [$responseTime%.1f msec]")
      case it: Iterator[_] =>
        val rows = it.toList
        logger.info(f"$label ~> (${rows.size} items) [$responseTime%.1f msec]")
        rows.zipWithIndex.foreach { case (row, index) => logger.info(f"[$index%02d] $row") }
      case rows: Seq[_] =>
        logger.info(f"$label ~> (${rows.size} items) [$responseTime%.1f msec]")
        rows.zipWithIndex.foreach { case (row, index) => logger.info(f"[$index%02d] $row") }
      case results: QueryResponse =>
        results.tabulate(20).foreach(logger.info)
      case result =>
        logger.info(f"$label ~> $result [$responseTime%.1f msec]")
    }
    results
  }

  private def toHexString(bytes: Array[Byte]): String = {
    val hex = bytes.map(b => String.format("%02x", b)).mkString(".")
    val ascii = String.valueOf(bytes.map(_.toChar) map {
      case b if b >= 20 && b <= 127 => b
      case _ => '.'
    })
    s"$ascii - $hex"
  }

}
