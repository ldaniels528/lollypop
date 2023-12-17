package com.lollypop.database.jdbc

import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, _}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import java.math.RoundingMode
import java.sql.DriverManager
import java.util.Calendar

class JDBCResultSetTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val ref = DatabaseObjectRef(getTestTableName)

  describe(classOf[JDBCResultSet].getSimpleName) {

    it("should retrieve results from a table") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // verify all rows
        assert(!rs.wasNull())
        assert(rs.isBeforeFirst)
        assert(rs.next())
        assert(rs.getString("symbol") == "BXXG")
        assert(rs.getString("exchange") == "NASDAQ")
        assert(rs.getDouble("lastSale") == 147.63)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1596317591000L)
        assert(rs.next())
        assert(rs.getString("symbol") == "KFFQ")
        assert(rs.getString("exchange") == "NYSE")
        assert(rs.getFloat("lastSale") == 22.92f)
        assert(rs.getDate("lastSaleTime").getTime == 1596317591000L)
        assert(rs.next())
        assert(rs.getString("symbol") == "GTKK")
        assert(rs.getString("exchange") == "NASDAQ")
        assert(rs.getInt("lastSale") == 240)
        assert(rs.getTime("lastSaleTime").getTime == 1596835991000L)
        assert(rs.next())
        assert(rs.getString("symbol") == "KNOW")
        assert(rs.getString("exchange") == "OTCBB")
        assert(rs.getLong("lastSale") == 357L)
        assert(rs.getRef("lastSaleTime").getObject == new java.sql.Date(1597872791000L))
        assert(!rs.next())
        assert(rs.isAfterLast)
      }
    }

    it("should move the cursor to the last row then back to the first row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row
        assert(rs.last())
        assert(rs.isLast)
        assert(rs.getString("symbol") == "KNOW")
        assert(rs.getString("exchange") == "OTCBB")
        assert(rs.getDouble("lastSale") == 357.21)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1597872791000L)
        assert(!rs.next())

        // move cursor back to the first row
        assert(rs.first())
        assert(rs.isFirst)
        assert(rs.getString("symbol") == "BXXG")
        assert(rs.getString("exchange") == "NASDAQ")
        assert(rs.getBigDecimal("lastSale").setScale(4, RoundingMode.UP).doubleValue() == 147.6300)
        assert(rs.getDate("lastSaleTime", Calendar.getInstance()).getTime == 1596317591000L)
        assert(rs.next())
      }
    }

    it("should move the cursor to an absolute row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row
        assert(rs.absolute(3))
        assert(rs.getString("symbol") == "GTKK")
        assert(rs.getString("exchange") == "NASDAQ")
        assert(rs.getDouble("lastSale") == 240.14)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1596835991000L)
        assert(rs.next())
      }
    }

    it("should move the cursor to a relative row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row
        assert(rs.last())
        assert(rs.relative(-2))
        assert(rs.getRow == 2)
        assert(rs.getString("symbol") == "KFFQ")
        assert(rs.getString("exchange") == "NYSE")
        assert(rs.getDouble("lastSale") == 22.92)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1596317591000L)
        assert(rs.next())
      }
    }

    it("should move the cursor to the previous row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row
        assert(rs.last())
        assert(rs.previous())
        assert(rs.getRow == 3)
        assert(rs.getString("symbol") == "GTKK")
        assert(rs.getString("exchange") == "NASDAQ")
        assert(rs.getDouble("lastSale") == 240.14)
        assert(rs.getObject("lastSaleTime") == new java.sql.Date(1596835991000L))
        assert(rs.next())
      }
    }

    it("should move the cursor to the outer extents") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row
        rs.afterLast()
        assert(rs.getRow == 5)
        rs.beforeFirst()
        assert(rs.getRow == 0)
      }
    }

    it("should update a row in the result set") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          """||---------------------------------------------------------|
             || symbol | exchange | lastSale | lastSaleTime             |
             ||---------------------------------------------------------|
             || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
             || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
             || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
             || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
             ||---------------------------------------------------------|
             |""".stripMargin)

        // move cursor to the last row and update it
        assert(rs.last())
        rs.updateString("exchange", "AMEX")
        rs.updateDouble("lastSale", 356.99)
        rs.updateTimestamp("lastSaleTime", DateHelper("2020-08-19T21:33:12.000Z").toTimestamp)
        rs.updateRow()

        // verify the row
        assert(rs.getString("symbol") == "KNOW")
        assert(rs.getString("exchange") == "AMEX")
        assert(rs.getDouble("lastSale") == 356.99)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1597872792000L)
        assert(!rs.next())
      }
    }

    it("should create a durable table for testing") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val count = conn.createStatement().executeUpdate(
          s"""|drop if exists $ref
              |create table $ref (
              |  symbol: String(5),
              |  exchange: String(6),
              |  lastSale: Double,
              |  lastSaleTime: DateTime
              |) containing (
              ||---------------------------------------------------------|
              || symbol | exchange | lastSale | lastSaleTime             |
              ||---------------------------------------------------------|
              || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
              || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
              || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
              || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
              ||---------------------------------------------------------|
              |)
              |""".stripMargin)
        assert(count == 9)
      }
    }

    it("should insert a new row via insertRow()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // move cursor to the 2nd row and create a modified copy of it
        assert(rs.absolute(2))
        rs.updateString("symbol", "WKRP")
        rs.updateDouble("lastSale", 22.97)
        rs.updateTimestamp("lastSaleTime", DateHelper("2020-08-19T21:33:12.000Z").toTimestamp)
        rs.insertRow()
        assert(rs.rowInserted())
      }
    }

    it("should retrieve the previously inserted row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref where symbol is 'WKRP'
              |""".stripMargin)

        // verify the row
        assert(!rs.wasNull())
        assert(rs.next())
        assert(rs.getString("symbol") == "WKRP")
        assert(rs.getString("exchange") == "NYSE")
        assert(rs.getDouble("lastSale") == 22.97)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1597847592000L)
        assert(!rs.next())
      }
    }

    it("should retrieve all rows (after insert)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // verify the row
        val rc = rs.toRowCollection
        rc.tabulate().foreach(logger.info)
        assert(rc.toMapGraph == List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper("2020-08-01T21:33:11.000Z")),
          Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper("2020-08-01T21:33:11.000Z")),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper("2020-08-07T21:33:11.000Z")),
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper("2020-08-19T21:33:11.000Z")),
          Map("symbol" -> "WKRP", "exchange" -> "NYSE", "lastSale" -> 22.97, "lastSaleTime" -> DateHelper("2020-08-19T14:33:12.000Z"))
        ))
      }
    }

    it("should update an existing row via updateRow()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // move cursor to the 2nd row and create a modified copy of it
        assert(rs.absolute(2))
        rs.updateString("symbol", "IBM")
        rs.updateDouble("lastSale", 188.11)
        rs.updateTimestamp("lastSaleTime", DateHelper("2020-08-19T21:33:13.000Z").toTimestamp)
        rs.updateRow()
        assert(rs.rowUpdated())
      }
    }

    it("should retrieve the previously updated row") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref where symbol is 'IBM'
              |""".stripMargin)

        // verify the row
        assert(!rs.wasNull())
        assert(rs.next())
        assert(rs.getString("symbol") == "IBM")
        assert(rs.getString("exchange") == "NYSE")
        assert(rs.getDouble("lastSale") == 188.11)
        assert(rs.getTimestamp("lastSaleTime").getTime == 1597847593000L)
        assert(!rs.next())
      }
    }

    it("should retrieve all rows (after update)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // verify the row
        val rc = rs.toRowCollection
        rc.tabulate().foreach(logger.info)
        assert(rc.toMapGraph == List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper("2020-08-01T21:33:11.000Z")),
          Map("symbol" -> "IBM", "exchange" -> "NYSE", "lastSale" -> 188.11, "lastSaleTime" -> DateHelper("2020-08-19T14:33:13.000Z")),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper("2020-08-07T21:33:11.000Z")),
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper("2020-08-19T21:33:11.000Z")),
          Map("symbol" -> "WKRP", "exchange" -> "NYSE", "lastSale" -> 22.97, "lastSaleTime" -> DateHelper("2020-08-19T14:33:12.000Z"))
        ))
      }
    }

    it("should delete an existing row via deleteRow()") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // move cursor to the 4th row and create a modified copy of it
        assert(rs.absolute(4))
        rs.deleteRow()
        assert(rs.rowDeleted())
      }
    }

    it("should retrieve all rows (after delete)") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $ref
              |""".stripMargin)

        // verify the row
        val rc = rs.toRowCollection
        rc.tabulate().foreach(logger.info)
        assert(rc.toMapGraph == List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper("2020-08-01T21:33:11.000Z")),
          Map("symbol" -> "IBM", "exchange" -> "NYSE", "lastSale" -> 188.11, "lastSaleTime" -> DateHelper("2020-08-19T14:33:13.000Z")),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper("2020-08-07T21:33:11.000Z")),
          Map("symbol" -> "WKRP", "exchange" -> "NYSE", "lastSale" -> 22.97, "lastSaleTime" -> DateHelper("2020-08-19T14:33:12.000Z"))
        ))
      }
    }

  }

}
