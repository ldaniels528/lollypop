package com.lollypop.database.jdbc

import com.lollypop.runtime._
import org.scalatest.funspec.AnyFunSpec

import java.sql.{DriverManager, ParameterMetaData}

class JDBCParameterMetaDataTest extends AnyFunSpec with JDBCTestServer {
  val tableName = "samples.jdbc.stocks_pmd"

  describe(classOf[JDBCParameterMetaData].getSimpleName) {

    it("should support parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime, isActive)
              |values (?, ?, ?, ?, ?)
              |""".stripMargin)
        val timestamp = new java.sql.Timestamp(System.currentTimeMillis())
        var n = 0
        Seq(("AMZN", "NYSE", 56.55, timestamp, true), ("GOOG", "NASDAQ", 98.55, timestamp, true),
          ("ABC", "OTCBB", 0.22, timestamp, false), ("AMD", "NASDAQ", 78.99, timestamp, true)) foreach {
          case (symbol, exchange, lastSale, lastSaleTime, isActive) =>
            ps.setString(n + 1, symbol)
            ps.setString(n + 2, exchange)
            ps.setDouble(n + 3, lastSale)
            ps.setTimestamp(n + 4, lastSaleTime)
            ps.setBoolean(n + 5, isActive)
            n += 5
        }

        // verify the parameter count
        val pmd = ps.getParameterMetaData
        assert(pmd.getParameterCount == 20)

        // verify a parameter's nullability
        assert(pmd.isNullable(1) == ParameterMetaData.parameterNullable)

        // verify the parameter mode
        assert(pmd.getParameterMode(1) == ParameterMetaData.parameterModeIn)

        // verify the parameter precision
        assert(pmd.getPrecision(1) == 4)

        // verify whether the parameter is signed
        assert(!pmd.isSigned(2))
        assert(pmd.isSigned(3))

        // verify the parameter scale
        assert(pmd.getScale(1) == 0)

        // verify the parameter types
        assert(pmd.getParameterType(1) == java.sql.Types.VARCHAR)
        assert(pmd.getParameterType(2) == java.sql.Types.VARCHAR)
        assert(pmd.getParameterType(3) == java.sql.Types.DOUBLE)
        assert(pmd.getParameterType(4) == java.sql.Types.TIMESTAMP)
        assert(pmd.getParameterType(5) == java.sql.Types.BOOLEAN)

        // verify the parameter type name
        assert(pmd.getParameterTypeName(1) == "String")

        // verify the parameter class name
        assert(pmd.getParameterClassName(1) == "java.lang.String")

        // verify the parameters are cleared
        ps.clearParameters()
        assert(pmd.getParameterCount == 0)
      }
    }

    it("should create a parameterized table") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|drop if exists $tableName
              |create table if not exists $tableName (
              |  symbol: String(?),
              |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
              |  lastSale: Double,
              |  lastSaleTime: DateTime,
              |  isActive: Boolean
              |)
              |""".stripMargin)
        ps.setInt(1, 8)
        val created = ps.execute()
        assert(created)
      }
    }

    it("should insert parameterized records") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // insert some records
        val ps = conn.prepareStatement(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime, isActive)
              |values (?, ?, ?, ?, ?),
              |       (?, ?, ?, ?, ?)
              |""".stripMargin)
        val now = System.currentTimeMillis()
        var n = 0
        Seq(("AMZN", "NYSE", 56.55, now, true), ("GOOG", "NASDAQ", 98.55, now, true)) foreach {
          case (symbol, exchange, lastSale, lastSaleTime, isActive) =>
            ps.setString(n + 1, symbol)
            ps.setString(n + 2, exchange)
            ps.setDouble(n + 3, lastSale)
            ps.setTimestamp(n + 4, new java.sql.Timestamp(lastSaleTime))
            ps.setBoolean(n + 5, isActive)
            n += 5
        }

        // execute and verify the insert count
        val count = ps.executeUpdate()
        assert(count == 2)
      }
    }

    it("should insert a BATCH of parameterized records") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // insert some records
        val ps = conn.prepareStatement(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime, isActive)
              |values (?, ?, ?, ?, ?)
              |""".stripMargin)
        val now = System.currentTimeMillis()
        Seq(("ABC", "OTCBB", 0.22, now, false), ("AMD", "NASDAQ", 78.99, now, true)) foreach {
          case (symbol, exchange, lastSale, lastSaleTime, isActive) =>
            ps.setString(1, symbol)
            ps.setString(2, exchange)
            ps.setDouble(3, lastSale)
            ps.setTimestamp(4, new java.sql.Timestamp(lastSaleTime))
            ps.setBoolean(5, isActive)
            ps.addBatch()
        }

        // execute and verify the insert count
        val count = ps.executeBatch()
        assert(count.length == 2 && count.forall(_ == 1))
      }
    }

    it("should execute a parameterized query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // verify the data
        // AMEX = 0, NASDAQ = 1, NYSE = 2, OTCBB = 3, OTHEROTC = 4
        val ps = conn.prepareStatement(
          s"""|select *
              |from $tableName
              |where symbol is ?
              |""".stripMargin)
        ps.setString(1, "AMZN")
        val rs = ps.executeQuery()
        assert(rs.next())
        assert(rs.getString("symbol") == "AMZN")
        assert(rs.getShort("exchange") == 2)
        assert(rs.getDouble("lastSale") == 56.55)
        assert(rs.getBoolean("isActive"))
        assert(!rs.next())
      }
    }

  }

}
