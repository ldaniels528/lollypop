package com.qwery.database.jdbc.types

import com.qwery.database.jdbc.JDBCTestServer
import com.qwery.database.jdbc.types.JDBCStructTest.JDBCArrayConversion
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.util.DateHelper
import com.qwery.util.ResourceHelper._
import com.qwery.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager

/**
 * JDBC Struct Test Suite
 */
class JDBCStructTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val tableName = getTestTableName

  describe(classOf[JDBCStruct].getSimpleName) {

    it("should retrieve records from the nested inner table as Arrays of Struct") {
      createDataFrame()

      //  read columns with an nested table
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs1 = conn.createStatement().executeQuery(
          s"""|select symbol, exchange, transactions
              |from $tableName
              |order by symbol asc
              |""".stripMargin)
        assert(rs1.next())
        assert(rs1.getString(1) == "AAPL")
        assert(rs1.getString(2) == "NASDAQ")
        assert(rs1.getArray(3).unwrapStruct == Seq(
          Seq(156.39, DateHelper("2021-08-05T19:23:11.000Z"))
        ))
        assert(rs1.next())
        assert(rs1.getString(1) == "AMD")
        assert(rs1.getString(2) == "NASDAQ")
        assert(rs1.getArray(3).unwrapStruct == Seq(
          Seq(56.87, DateHelper("2021-08-05T19:23:11.000Z"))
        ))
        assert(rs1.next())
        assert(rs1.getString(1) == "AMZN")
        assert(rs1.getString(2) == "NASDAQ")
        assert(rs1.getArray(3).unwrapStruct == Seq(
          Seq(988.12, DateHelper("2021-08-05T19:23:11.000Z"))
        ))
        assert(rs1.next())
        assert(rs1.getString(1) == "INTC")
        assert(rs1.getString(2) == "NYSE")
        assert(rs1.getArray(3).unwrapStruct == Seq(
          Seq(89.44, DateHelper("2021-08-05T19:23:11.000Z"))
        ))

        val transactions = rs1.getArray("transactions")
        assert(transactions.getArray().isInstanceOf[Array[java.sql.Struct]])
        val results = transactions.getArray().asInstanceOf[Array[java.sql.Struct]]
        logger.info(s"results: ${results.render}")
      }
    }

    it("should retrieve records from the nested inner table as Structs") {
      createDataFrame()

      //  read columns with an nested table
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs1 = conn.createStatement().executeQuery(
          s"""|select symbol, exchange, transactions
              |from $tableName
              |order by symbol asc
              |""".stripMargin)
        assert(rs1.next())
        assert(rs1.getString(1) == "AAPL")
        assert(rs1.getString(2) == "NASDAQ")
        rs1.getObject(3) match {
          case s: java.sql.Struct =>
            assert(s.getAttributes.toSeq == Seq(Map("price" -> 156.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))))
          case x => fail(s"Expected java.sql.Struct but got $x")
        }
        assert(rs1.next())
        assert(rs1.getString(1) == "AMD")
        assert(rs1.getString(2) == "NASDAQ")
        rs1.getObject(3) match {
          case s: java.sql.Struct =>
            assert(s.getAttributes.toSeq == Seq(Map("price" -> 56.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))))
          case x => fail(s"Expected java.sql.Struct but got $x")
        }
        assert(rs1.next())
        assert(rs1.getString(1) == "AMZN")
        assert(rs1.getString(2) == "NASDAQ")
        rs1.getObject(3) match {
          case s: java.sql.Struct =>
            assert(s.getAttributes.toSeq == Seq(Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))))
          case x => fail(s"Expected java.sql.Struct but got $x")
        }
        assert(rs1.next())
        assert(rs1.getString(1) == "INTC")
        assert(rs1.getString(2) == "NYSE")
        rs1.getObject(3) match {
          case s: java.sql.Struct =>
            assert(s.getAttributes.toSeq == Seq(Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))))
          case x => fail(s"Expected java.sql.Struct but got $x")
        }

        val transactions = rs1.getArray("transactions")
        assert(transactions.getArray().isInstanceOf[Array[java.sql.Struct]])
        val results = transactions.getArray().asInstanceOf[Array[java.sql.Struct]]
        logger.info(s"results: ${results.render}")
      }
    }

    it("should retrieve records from the nested inner table as Strings") {
      createDataFrame()

      // toJson columns from an nested table
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs2 = conn.createStatement().executeQuery(
          s"""|select symbol, exchange, transactions: transactions.toJsonString()
              |from $tableName
              |order by symbol asc
              |""".stripMargin)
        assert(rs2.next())
        assert(rs2.getString("symbol") == "AAPL")
        assert(rs2.getString("exchange") == "NASDAQ")
        assert(rs2.getString("transactions") == """[{"price":156.39,"transactionTime":"2021-08-05T19:23:11.000Z"}]""")
        assert(rs2.next())
        assert(rs2.getString("symbol") == "AMD")
        assert(rs2.getString("exchange") == "NASDAQ")
        assert(rs2.getString("transactions") == """[{"price":56.87,"transactionTime":"2021-08-05T19:23:11.000Z"}]""")
        assert(rs2.next())
        assert(rs2.getString("symbol") == "AMZN")
        assert(rs2.getString("exchange") == "NASDAQ")
        assert(rs2.getString("transactions") == """[{"price":988.12,"transactionTime":"2021-08-05T19:23:11.000Z"}]""")
        assert(rs2.next())
        assert(rs2.getString("symbol") == "INTC")
        assert(rs2.getString("exchange") == "NYSE")
        assert(rs2.getString("transactions") == """[{"price":89.44,"transactionTime":"2021-08-05T19:23:11.000Z"}]""")
      }
    }

    it("should retrieve records from the nested inner table as individual objects (UNNEST)") {
      createDataFrame()

      // test the the UNNEST function
      DriverManager.getConnection(jdbcURL) use { conn =>
        val rs3 = conn.createStatement().executeQuery(
          s"""|select symbol, exchange, unnest(transactions)
              |from $tableName
              |where symbol is 'SHMN'
              |""".stripMargin)
        assert(rs3.next())
        assert(rs3.getString("symbol") == "SHMN")
        assert(rs3.getString("exchange") == "OTCBB")
        assert(rs3.getDouble("price") == 0.001)
        assert(rs3.getDate("transactionTime").getTime == 1628191391000L)
        assert(rs3.next())
        assert(rs3.getString("symbol") == "SHMN")
        assert(rs3.getString("exchange") == "OTCBB")
        assert(rs3.getDouble("price") == 0.0011)
        assert(rs3.getTimestamp("transactionTime").getTime == 1628191392000L)
      }
    }

  }

  private def createDataFrame(): Int = {
    DriverManager.getConnection(jdbcURL) use { conn =>
      conn.createStatement().executeUpdate(
        s"""|drop if exists $tableName
            |create table $tableName (
            |   symbol: String(5),
            |   exchange: String(6),
            |   transactions: Table(
            |       price: Double,
            |       transactionTime: DateTime
            |   )[10]
            |)
            |insert into $tableName (symbol, exchange, transactions)
            |values ('AAPL', 'NASDAQ', '{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMD', 'NASDAQ', '{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('INTC','NYSE', '{"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('AMZN', 'NASDAQ', '{"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
            |       ('SHMN', 'OTCBB', '[{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                           {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]')
            |""".stripMargin)
    }
  }

}

object JDBCStructTest {

  /**
   * JDBC Array Conversion
   * @param array the host [[java.sql.Array array]]
   */
  final implicit class JDBCArrayConversion(val array: java.sql.Array) extends AnyVal {

    @inline
    def unwrapStruct: Seq[Seq[AnyRef]] = array.getArray.asInstanceOf[Array[java.sql.Struct]].map(_.getAttributes.toSeq).toSeq

  }

}
