package com.qwery.database
package jdbc

import com.qwery.AppConstants._
import com.qwery.runtime.DatabaseObjectRef
import com.qwery.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager

class JDBCDatabaseMetaDataTest extends AnyFunSpec with JDBCTestServer {

  describe(classOf[JDBCDatabaseMetaData].getSimpleName) {

    it("should provide basic JDBC driver information") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val dmd = conn.getMetaData
        logger.info(s"Driver Name:        ${dmd.getDriverName}")
        logger.info(s"Product Name:       ${dmd.getDatabaseProductName}")
        logger.info(s"JDBC Major Version: ${dmd.getJDBCMajorVersion}")
        logger.info(s"JDBC Minor Version: ${dmd.getJDBCMinorVersion}")
        assert(dmd.getDriverName == s"Qwery v$MAJOR_VERSION.$MINOR_VERSION.$MINI_VERSION")
        assert(dmd.getDatabaseProductName == "Qwery")
        assert(dmd.getJDBCMajorVersion == MAJOR_VERSION)
        assert(dmd.getJDBCMinorVersion == MINOR_VERSION)
      }
    }

    it("should retrieve the client info properties (getClientInfoProperties)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val props = conn.getMetaData.getClientInfoProperties.toRowCollection
        props.tabulate() foreach logger.info
        assert(props.toMapGraph == List(
          Map("NAME" -> "database", "MAX_LEN" -> 1024, "DEFAULT_VALUE" -> DEFAULT_DATABASE, "DESCRIPTION" -> "the name of the database"),
          Map("NAME" -> "schema", "MAX_LEN" -> 1024, "DEFAULT_VALUE" -> DEFAULT_SCHEMA, "DESCRIPTION" -> "the name of the schema"),
          Map("NAME" -> "server", "MAX_LEN" -> 1024, "DEFAULT_VALUE" -> DEFAULT_HOST, "DESCRIPTION" -> "the hostname of the server"),
          Map("NAME" -> "port", "MAX_LEN" -> 5, "DEFAULT_VALUE" -> DEFAULT_PORT, "DESCRIPTION" -> "the port of the server")
        ))
      }
    }

    it("should retrieve the list of databases (getCatalogs)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.getMetaData.getCatalogs.toRowCollection.tabulate() foreach logger.info
      }
    }

    it("should retrieve the list of table types (getTableTypes)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val tableTypes = conn.getMetaData.getTableTypes.toRowCollection
        tableTypes.tabulate() foreach logger.info
        assert(tableTypes.toMapGraph == List(
          Map("TABLE_TYPE" -> "external table"),
          Map("TABLE_TYPE" -> "table"),
          Map("TABLE_TYPE" -> "index"),
          Map("TABLE_TYPE" -> "view")
        ))
      }
    }

    it("should retrieve the columns for a given database (getColumns)") {
      val ref = DatabaseObjectRef(classOf[JDBCDatabaseMetaDataTest].getSimpleName)
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|namespace "temp.jdbc"
              |drop if exists $ref
              |create table $ref (
              |  symbol: String(4),
              |  exchange: String(6),
              |  lastSale: Float,
              |  lastSaleTime: DateTime
              |)
              |insert into $ref (symbol, exchange, lastSale, lastSaleTime)
              |values ('AAPL', 'NYSE',   67.11, DateTime('2022-09-04T23:36:47.846Z')),
              |       ('AMD',  'NASDAQ', 98.76, DateTime('2022-09-04T23:36:47.975Z')),
              |       ('YHOO', 'NYSE',   23.89, DateTime('2022-09-04T23:36:47.979Z'))
              |""".stripMargin)
        assert(count == 3)
        val columns = Set("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "TYPE_NAME", "COLUMN_SIZE")
        val results = conn.getMetaData.getColumns(null, null, ref.toSQL, null).toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph.map(_.filter { case (key, _) => columns.contains(key) }) == List(
          Map("TYPE_NAME" -> "String", "TABLE_CAT" -> "temp", "COLUMN_SIZE" -> 4, "COLUMN_NAME" -> "symbol", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_SCHEM" -> "jdbc"),
          Map("TYPE_NAME" -> "String", "TABLE_CAT" -> "temp", "COLUMN_SIZE" -> 6, "COLUMN_NAME" -> "exchange", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_SCHEM" -> "jdbc"),
          Map("TYPE_NAME" -> "Float", "TABLE_CAT" -> "temp", "COLUMN_SIZE" -> 4, "COLUMN_NAME" -> "lastSale", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_SCHEM" -> "jdbc"),
          Map("TYPE_NAME" -> "DateTime", "TABLE_CAT" -> "temp", "COLUMN_SIZE" -> 8, "COLUMN_NAME" -> "lastSaleTime", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_SCHEM" -> "jdbc")
        ))
      }
    }

    it("should retrieve the list of indices (getIndexInfo)") {
      val ref = DatabaseObjectRef(classOf[JDBCDatabaseMetaDataTest].getSimpleName)
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeUpdate(
          s"""|namespace "temp.jdbc"
              |drop if exists $ref
              |create table $ref (
              |  symbol: String(5),
              |  exchange: String(6),
              |  lastSale: Float,
              |  lastSaleTime: DateTime
              |)
              |insert into $ref (symbol, exchange, lastSale, lastSaleTime)
              |values ('AAPL', 'NYSE',   67.11, DateTime('2022-09-04T23:36:47.846Z')),
              |       ('AMD',  'NASDAQ', 98.76, DateTime('2022-09-04T23:36:47.975Z')),
              |       ('YHOO', 'NYSE',   23.89, DateTime('2022-09-04T23:36:47.979Z'))
              |create index $ref#symbol
              |""".stripMargin)
        val results = conn.getMetaData.getIndexInfo(null, null, ref.toSQL, false, false).toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph == List(Map(
          "PAGES" -> 1, "TABLE_CAT" -> "temp", "INDEX_QUALIFIER" -> true, "ASC_OR_DESC" -> "ascending",
          "FILTER_CONDITION" -> "", "COLUMN_NAME" -> "symbol", "ORDINAL_POSITION" -> 0, "CARDINALITY" -> 0,
          "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_SCHEM" -> "jdbc", "NON_UNIQUE" -> true, "TYPE" -> "tableIndexHashed")
        ))
      }
    }

    it("should retrieve the list of procedures (getProcedures)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|namespace "temp.jdbc"
              |declare table results(symbol: String(5), exchange: String(6), lastSale: Double)[5]
              |insert into @@results (symbol, exchange, lastSale)
              |values ('GMTQ', 'OTCBB', 0.1111), ('ABC', 'NYSE', 38.47), ('GE', 'NASDAQ', 57.89)
              |
              |drop if exists getStockQuote
              |create procedure getStockQuote(theExchange: String) := {
              |    stdout <=== 'Selected Exchange: "{{ @theExchange }}"'
              |    select exchange, count(*) as total, max(lastSale) as maxPrice, min(lastSale) as minPrice
              |    from @@results
              |    where exchange == @theExchange
              |    group by exchange
              |}
              |
              |call getStockQuote('NASDAQ')
              |""".stripMargin)
        val device = rs.toRowCollection
        assert(device.toMapGraph == List(Map("exchange" -> "NASDAQ", "total" -> 1, "maxPrice" -> 57.89, "minPrice" -> 57.89)))
        val procedures = conn.getMetaData.getProcedures("temp", "jdbc", null).toRowCollection
        procedures.tabulate() foreach logger.info
        assert(procedures.toMapGraph == List(Map(
          "PROCEDURE_SCHEM" -> "jdbc", "PROCEDURE_TYPE" -> "procedureReturnsResult", "PROCEDURE_NAME" -> "getStockQuote",
          "SPECIFIC_NAME" -> "temp.jdbc.getStockQuote", "REMARKS" -> "", "PROCEDURE_CAT" -> "temp"
        )))
      }
    }

    it("should retrieve the list of schemas (getSchemas)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.getMetaData.getSchemas.tabulate() foreach logger.info
      }
    }

    it("should retrieve the tables for a given database (getTables)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val results = conn.getMetaData.getTables("temp", "jdbc", null, null)
          .toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph
          .map(_.filter { case (_, value) => value != null })
          .filter(_.exists { case (k, v) => k == "TYPE_NAME" && v == "JDBCDatabaseMetaDataTest" }) == List(
          Map("TYPE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_CAT" -> "temp", "TYPE_SCHEM" -> "jdbc", "TABLE_TYPE" -> "table", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "REMARKS" -> "", "TYPE_CAT" -> "temp", "TABLE_SCHEM" -> "jdbc"),
          Map("TYPE_NAME" -> "JDBCDatabaseMetaDataTest", "TABLE_CAT" -> "temp", "TYPE_SCHEM" -> "jdbc", "TABLE_TYPE" -> "index", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "REMARKS" -> "", "TYPE_CAT" -> "temp", "TABLE_SCHEM" -> "jdbc")
        ))
      }
    }

    it("should retrieve the table privileges for a given database (getTablePrivileges)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val results = conn.getMetaData.getTablePrivileges("temp", "jdbc", null)
          .toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph
          .filter(_.exists { case (k, v) => k == "TABLE_NAME" && v == "JDBCDatabaseMetaDataTest" }) == List(
          Map("GRANTEE" -> "admin", "TABLE_CAT" -> "temp", "GRANTOR" -> "admin", "IS_GRANTABLE" -> "NO", "TABLE_NAME" -> "JDBCDatabaseMetaDataTest", "PRIVILEGE" -> "select, insert, update, delete", "TABLE_SCHEM" -> "jdbc")
        ))
      }
    }

    it("should retrieve the stored functions for a given database (getFunctions)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val results = conn.getMetaData.getFunctions("temp", "jdbc", null)
          .toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph == Nil)
      }
    }

    it("should retrieve the user-defined types for a given database (getUDTs)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val results = conn.getMetaData.getUDTs("temp", "jdbc", null, null)
          .toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph == Nil)
      }
    }

    it("should retrieve the data type information (getTypeInfo)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val results = conn.getMetaData.getTypeInfo.toRowCollection
        results.tabulate() foreach logger.info
        assert(results.toMapGraph.map(_.filter { case (_, value) => value != null }) == List(
          Map("PRECISION" -> 256, "TYPE_NAME" -> "BitArray", "DATA_TYPE" -> -3, "NULLABLE" -> 1),
          Map("PRECISION" -> 24, "TYPE_NAME" -> "BLOB", "DATA_TYPE" -> 2004, "NULLABLE" -> 1),
          Map("PRECISION" -> 1, "TYPE_NAME" -> "Boolean", "DATA_TYPE" -> 16, "NULLABLE" -> 1),
          Map("PRECISION" -> 2, "TYPE_NAME" -> "Char", "DATA_TYPE" -> 1, "NULLABLE" -> 1),
          Map("PRECISION" -> 24, "TYPE_NAME" -> "CLOB", "DATA_TYPE" -> 2005, "NULLABLE" -> 1),
          Map("PRECISION" -> 8, "TYPE_NAME" -> "DateTime", "DATA_TYPE" -> 93, "NULLABLE" -> 1),
          Map("PRECISION" -> 2, "TYPE_NAME" -> "Enum", "DATA_TYPE" -> 5, "NULLABLE" -> 1),
          Map("PRECISION" -> 1, "TYPE_NAME" -> "Byte", "DATA_TYPE" -> -6, "NULLABLE" -> 1),
          Map("PRECISION" -> 2, "TYPE_NAME" -> "Short", "DATA_TYPE" -> 5, "NULLABLE" -> 1),
          Map("PRECISION" -> 4, "TYPE_NAME" -> "Int", "DATA_TYPE" -> 4, "NULLABLE" -> 1),
          Map("PRECISION" -> 8, "TYPE_NAME" -> "Long", "DATA_TYPE" -> -5, "NULLABLE" -> 1),
          Map("PRECISION" -> 4, "TYPE_NAME" -> "Float", "DATA_TYPE" -> 6, "NULLABLE" -> 1),
          Map("PRECISION" -> 8, "TYPE_NAME" -> "Double", "DATA_TYPE" -> 8, "NULLABLE" -> 1),
          Map("PRECISION" -> 128, "TYPE_NAME" -> "Numeric", "DATA_TYPE" -> 2, "NULLABLE" -> 1),
          Map("PRECISION" -> 12, "TYPE_NAME" -> "Interval", "DATA_TYPE" -> -5, "NULLABLE" -> 1),
          Map("PRECISION" -> 256, "TYPE_NAME" -> "JSON", "DATA_TYPE" -> 12, "NULLABLE" -> 1),
          Map("PRECISION" -> 24, "TYPE_NAME" -> "Pointer", "DATA_TYPE" -> 2000, "NULLABLE" -> 1),
          Map("PRECISION" -> 17, "TYPE_NAME" -> "RowIDRange", "DATA_TYPE" -> 2003, "NULLABLE" -> 1),
          Map("PRECISION" -> 8, "TYPE_NAME" -> "RowNumber", "DATA_TYPE" -> java.sql.Types.ROWID, "NULLABLE" -> 1),
          Map("PRECISION" -> 24, "TYPE_NAME" -> "SQLXML", "DATA_TYPE" -> 2009, "NULLABLE" -> 1),
          Map("PRECISION" -> 16, "TYPE_NAME" -> "UUID", "DATA_TYPE" -> -2, "NULLABLE" -> 1),
          Map("PRECISION" -> 256, "TYPE_NAME" -> "String", "DATA_TYPE" -> 12, "NULLABLE" -> 1),
          Map("PRECISION" -> 8192, "TYPE_NAME" -> "VarBinary", "DATA_TYPE" -> -3, "NULLABLE" -> 1),
          Map("PRECISION" -> 8192, "TYPE_NAME" -> "VarChar", "DATA_TYPE" -> 12, "NULLABLE" -> 1),
          Map("PRECISION" -> 16384, "TYPE_NAME" -> "Any", "DATA_TYPE" -> 2000, "NULLABLE" -> 1)
        ))
      }
    }

  }

}
