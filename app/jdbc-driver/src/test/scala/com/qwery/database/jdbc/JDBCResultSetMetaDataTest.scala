package com.qwery.database.jdbc

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.sql.{DriverManager, ResultSetMetaData}
import scala.collection.concurrent.TrieMap

class JDBCResultSetMetaDataTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val cache = TrieMap[Unit, ResultSetMetaData]()

  describe(classOf[JDBCResultSetMetaData].getSimpleName) {

    it("should produce the correct number of columns (getColumnCount)") {
      val rsmd = getMetaData
      assert(rsmd.getColumnCount == 5)
    }

    it("should produce the correct column names (getColumnName)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getColumnName(n)
      assert(columns == Seq("stock_id", "symbol", "exchange", "lastSale", "lastSaleTime"))
    }

    it("should produce the correct column type names (getColumnTypeName)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getColumnTypeName(n)
      assert(columns == Seq("RowNumber", "String", "Enum", "Double", "DateTime"))
    }

    it("should produce the correct catalog name (getCatalogName)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getCatalogName(n)
      assert(columns == Seq("temp", "temp", "temp", "temp", "temp"))
    }

    it("should produce the correct column display size (getColumnDisplaySize)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getColumnDisplaySize(n)
      assert(columns == Seq(8, 5, 2, 8, 8))
    }

    it("should produce the correct column label (getColumnLabel)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getColumnLabel(n)
      assert(columns == Seq("stock_id", "symbol", "exchange", "lastSale", "lastSaleTime"))
    }

    it("should produce the correct precision (getPrecision)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getPrecision(n)
      assert(columns == Seq(8, 5, 2, 8, 8))
    }

    it("should produce the correct precision (getScale)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getScale(n)
      assert(columns == Seq(8, 5, 2, 8, 8))
    }

    it("should produce the correct schema name (getSchemaName)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getSchemaName(n)
      assert(columns == Seq("jdbc", "jdbc", "jdbc", "jdbc", "jdbc"))
    }

    it("should produce the correct table name (getTableName)") {
      val rsmd = getMetaData
      val columns = for {n <- 1 to rsmd.getColumnCount} yield rsmd.getTableName(n)
      assert(columns == Seq("JDBCResultSetMetaDataTest", "JDBCResultSetMetaDataTest", "JDBCResultSetMetaDataTest", "JDBCResultSetMetaDataTest", "JDBCResultSetMetaDataTest"))
    }

    it("should indicate which columns are auto-incrementing (isAutoIncrement)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isAutoIncrement(n) == (n == 1))
    }

    it("should indicate which columns are case-sensitive (isCaseSensitive)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isCaseSensitive(n))
    }

    it("should indicate which columns are currency types (isCurrency)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(!rsmd.isCurrency(n))
    }

    it("should indicate which columns are definitely writable (isDefinitelyWritable)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isDefinitelyWritable(n))
    }

    it("should indicate which columns are nullable (isNullable)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isNullable(n) == (if (n < 5) 1 else 2))
    }

    it("should indicate which columns are read-only (isReadOnly)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(!rsmd.isReadOnly(n))
    }

    it("should indicate which columns are searchable (isSearchable)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isSearchable(n))
    }

    it("should indicate which columns are signed (isSigned)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isSigned(n) == (n == 1 || n == 4))
    }

    it("should indicate which columns are writable (isWritable)") {
      val rsmd = getMetaData
      for (n <- 1 to rsmd.getColumnCount) assert(rsmd.isWritable(n))
    }

  }

  private def getMetaData: ResultSetMetaData = {
    cache.getOrElseUpdate((),
      DriverManager.getConnection(jdbcURL) use { conn =>
        val tableName = getTestTableName
        val rs = conn.createStatement().executeQuery(
          s"""|drop if exists $tableName &&
              |create table if not exists $tableName (
              |  stock_id: RowNumber,
              |  symbol: String(5),
              |  exchange: Enum ('AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHEROTC'),
              |  lastSale: Double,
              |  lastSaleTime: DateTime = DateTime()
              |) &&
              |insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values ("MSFT", "NYSE", 56.55, DateTime()),
              |       ("AAPL", "NASDAQ", 98.55, DateTime()),
              |       ("AMZN", "NYSE", 56.55, DateTime()),
              |       ("GOOG", "NASDAQ", 98.55, DateTime())
              |select * from $tableName
              |""".stripMargin)
        rs.getMetaData
      })
  }

}
