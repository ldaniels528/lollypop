package com.lollypop.database
package jdbc

import com.lollypop.runtime._
import com.lollypop.runtime.instructions.VerificationTools
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager

/**
 * JDBC Statement Test Suite
 */
class JDBCStatementTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val tableName: String = getTestTableName

  describe(classOf[JDBCStatement].getSimpleName) {

    it("should create a table") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val created = conn.createStatement().execute(
          s"""|drop if exists $tableName &&
              |create table if not exists $tableName (
              |  symbol: String(8),
              |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
              |  lastSale: Double,
              |  lastSaleTime: DateTime
              |)
              |""".stripMargin)
        assert(created)
      }
    }

    it("should insert records into it") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val stmt = conn.createStatement()
        stmt.addBatch(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values ("MSFT", "NYSE", 56.55, DateTime())
              |""".stripMargin)
        stmt.addBatch(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values ("AAPL", "NASDAQ", 98.55, DateTime())
              |""".stripMargin)
        stmt.addBatch(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values ("AMZN", "NYSE", 56.55, DateTime())
              |""".stripMargin)
        stmt.addBatch(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values ("GOOG", "NASDAQ", 98.55, DateTime())
              |""".stripMargin)
        val counts = stmt.executeBatch()
        assert(counts.length == 4 && counts.forall(_ == 1))
      }
    }

    it("should verify the inserted data") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |""".stripMargin) use { rs =>
          // AMEX = 0, NASDAQ = 1, NYSE = 2, OTCBB = 3, OTHEROTC = 4
          assert(rs.next())
          assert(rs.getString("symbol") == "MSFT")
          assert(rs.getShort("exchange") == 2)
          assert(rs.getDouble("lastSale") == 56.55)
          assert(rs.next())
          assert(rs.getString("symbol") == "AAPL")
          assert(rs.getShort("exchange") == 1)
          assert(rs.getDouble("lastSale") == 98.55)
          assert(rs.next())
          assert(rs.getString("symbol") == "AMZN")
          assert(rs.getShort("exchange") == 2)
          assert(rs.getDouble("lastSale") == 56.55)
          assert(rs.next())
          assert(rs.getString("symbol") == "GOOG")
          assert(rs.getShort("exchange") == 1)
          assert(rs.getDouble("lastSale") == 98.55)
          assert(!rs.next())
        }
      }
    }

    it("should execute an update statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|update $tableName set lastSale = 101.12, lastSaleTime = DateTime() where symbol is 'GOOG'
              |""".stripMargin)
        assert(count == 1)
      }
    }

    it("should execute a SELECT query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'AAPL'
              |""".stripMargin) use { rs =>
          assert(rs.next())
          assert(rs.getString("symbol") == "AAPL")
        }
      }
    }

    it("should execute a SELECT query with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'GOOG'
              |""".stripMargin) use { rs =>
          assert(rs.next())
          assert(rs.getString("symbol") == "GOOG")
        }
      }
    }

    it("should execute a count query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select transactions: count(*) from $tableName
              |""".stripMargin) use { rs =>
          assert(rs.next())
          assert(rs.getLong("transactions") == 4)
        }
      }
    }

    it("should execute a summarization query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select
              |   transactions: count(*),
              |   avgLastSale: avg(lastSale),
              |   minLastSale: min(lastSale),
              |   maxLastSale: max(lastSale),
              |   sumLastSale: sum(lastSale)
              |from $tableName
              |""".stripMargin) use { rs =>
          assert(rs.next())
          assert(rs.getDouble("sumLastSale") == 312.77)
          assert(rs.getDouble("maxLastSale") == 101.12)
          assert(rs.getDouble("avgLastSale") == 78.1925)
          assert(rs.getDouble("minLastSale") == 56.55)
          assert(rs.getLong("transactions") == 4)
        }
      }
    }

    it("should modify and insert a new record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'GOOG'
              |""".stripMargin) use { rs0 =>
          assert(rs0.next())
          rs0.updateString("symbol", "CCC")
          rs0.updateDouble("lastSale", 15.44)
          rs0.insertRow()
        }

        // retrieve the row again
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'CCC'
              |""".stripMargin) use { rs1 =>
          assert(rs1.next())
          assert(rs1.getString("symbol") == "CCC")
          assert(rs1.getDouble("lastSale") == 15.44)
        }
      }
    }

    it("should modify and update an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'GOOG'
              |""".stripMargin) use { rs0 =>
          assert(rs0.next())
          rs0.updateString("symbol", "AAA")
          rs0.updateDouble("lastSale", 78.99)
          rs0.updateLong("lastSaleTime", System.currentTimeMillis())
          rs0.updateRow()
          assert(rs0.rowUpdated())
        }

        // retrieve the row again
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'AAA'
              |""".stripMargin) use { rs1 =>
          assert(rs1.next())
          assert(rs1.getString("symbol") == "AAA")
          assert(rs1.getDouble("lastSale") == 78.99)
        }
      }
    }

    it("should delete an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |""".stripMargin) use { rs =>
          while (rs.next()) {
            // |---------------------------------------------------------|
            // | symbol | exchange | lastSale | lastSaleTime             |
            // |---------------------------------------------------------|
            // | AAPL   |        1 |    98.55 | 2022-10-06T19:34:14.731Z |
            // | AMZN   |        2 |    56.55 | 2022-10-06T19:34:14.731Z |
            // | GOOG   |        1 |   101.12 | 2022-10-06T19:34:14.773Z |
            // | CCC    |        1 |    15.44 | 2022-10-06T19:34:14.773Z |
            // |---------------------------------------------------------|
            if (rs.getString("symbol") == "CCC") {
              rs.deleteRow()
              assert(rs.rowDeleted())
            }
          }
        }

        // retrieve the row again
        conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |where symbol is 'CCC'
              |""".stripMargin) use { rs =>
          // AMEX = 0, NASDAQ = 1, NYSE = 2, OTCBB = 3, OTHEROTC = 4
          assert(!rs.next())
        }
      }
    }

  }

}
