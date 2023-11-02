package com.lollypop.database.jdbc

import com.lollypop.database.jdbc.types.JDBCRowId
import com.lollypop.database.jdbc.types.JDBCValueConversion.toJDBCType
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes.CLOB
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.util.CodecHelper.{RichInputStream, RichReader}
import com.lollypop.util.DateHelper
import com.lollypop.util.OptionHelper.OptionEnrichment
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.io.File
import java.sql.DriverManager
import scala.io.Source

class JDBCCallableStatementTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val testDir = new File("./app/jdbc-driver/src/test/scala/")
  private val files = testDir.streamFilesRecursively.filter(_.getName.endsWith(".scala")).sortBy(_.getName)

  describe(classOf[JDBCCallableStatement].getSimpleName) {

    it("should support a procedure with OUT parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val (tableName, procName) = getTableAndProcedureNames('B')

        // create and populate the table
        val cs0 = conn.prepareCall(
          s"""|drop if exists $tableName
              |create table $tableName (symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
              |containing (
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
        assert(cs0.executeUpdate() == 9)

        // create the procedure
        val cs1 = conn.prepareCall(
          s"""|drop if exists $procName
              |create procedure $procName(theExchange: String,
              |                           --> exchange: String,
              |                           --> total: Long,
              |                           --> maxPrice: Double,
              |                           --> minPrice: Double) := {
              |    select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
              |    from $tableName
              |    where exchange is theExchange
              |    group by exchange
              |}
              |""".stripMargin)
        assert(cs1.execute())

        // call the procedure
        val cs2 = conn.prepareCall(
          s"""|call $procName(?)
              |""".stripMargin)
        cs2.setObject(1, "NASDAQ")
        cs2.registerOutParameter("exchange", toJDBCType("String"))
        cs2.registerOutParameter("total", toJDBCType("Long"))
        cs2.registerOutParameter("maxPrice", toJDBCType("Double"))
        cs2.registerOutParameter("minPrice", toJDBCType("Double"))
        cs2.executeQuery()
        // verify by index
        assert(cs2.getObject(1) == "NASDAQ")
        assert(cs2.getString(1) == "NASDAQ")
        assert(cs2.getByte(2) == 2)
        assert(cs2.getShort(2) == 2)
        assert(cs2.getInt(2) == 2)
        assert(cs2.getLong(2) == 2)
        assert(cs2.getFloat(3) == 240.14f)
        assert(cs2.getDouble(3) == 240.14)
        assert(cs2.getBigDecimal(4) == new java.math.BigDecimal(147.63))
        // verify by name
        assert(cs2.getObject("exchange") == "NASDAQ")
        assert(cs2.getString("exchange") == "NASDAQ")
        assert(cs2.getByte("total") == 2)
        assert(cs2.getShort("total") == 2)
        assert(cs2.getInt("total") == 2)
        assert(cs2.getLong("total") == 2)
        assert(cs2.getFloat("maxPrice") == 240.14f)
        assert(cs2.getDouble("maxPrice") == 240.14)
        assert(cs2.getBigDecimal("minPrice") == new java.math.BigDecimal(147.63))
        assert(!cs2.wasNull())
      }
    }

    it("should support a procedure with date parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val (tableName, procName) = getTableAndProcedureNames('C')

        // create and populate the table
        val cs0 = conn.prepareCall(
          s"""|drop if exists $tableName
              |create table $tableName (symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
              |containing (
              ||---------------------------------------------------------|
              || symbol | exchange | lastSale | lastSaleTime             |
              ||---------------------------------------------------------|
              || SOUL   | NASDAQ   |    66.63 | 2020-08-19T21:33:11.000Z |
              || KFQ    | NYSE     |    28.29 | 2020-08-19T21:33:12.000Z |
              || GTK    | NASDAQ   |   240.14 | 2020-08-19T21:33:10.000Z |
              || CRY    | NASDAQ   |     3.21 | 2020-08-19T21:33:13.000Z |
              || AMC    | AMEX     |    78.11 | 2020-08-19T21:33:09.000Z |
              || TRX    | AMEX     |    17.76 | 2020-08-19T21:33:13.000Z |
              || FSB    | AMEX     |    45.67 | 2020-08-19T21:33:07.000Z |
              || XXX    | AMEX     |   111.11 | 2020-08-19T21:33:14.000Z |
              ||---------------------------------------------------------|
              |)
              |""".stripMargin)
        assert(cs0.executeUpdate() == 17)

        // create the procedure
        val cs1 = conn.prepareCall(
          s"""|drop if exists $procName
              |create procedure $procName(theExchange: String,
              |                           --> avgTime: DateTime,
              |                           --> maxTime: DateTime,
              |                           --> minTime: DateTime) := {
              |    select exchange, avgTime: avg(lastSaleTime), maxTime: max(lastSaleTime), minTime: min(lastSaleTime)
              |    from $tableName
              |    where exchange is theExchange
              |    group by exchange
              |}
              |""".stripMargin)
        assert(cs1.execute())

        // call the procedure
        val cs2 = conn.prepareCall(
          s"""|call $procName(?)
              |""".stripMargin)
        cs2.setString(1, "AMEX")
        cs2.registerOutParameter("avgTime", toJDBCType("DateTime"))
        cs2.registerOutParameter("maxTime", toJDBCType("DateTime"))
        cs2.registerOutParameter("minTime", toJDBCType("DateTime"))
        cs2.executeQuery()
        // verify by index
        assert(cs2.getDate(1).getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTime(1).getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTimestamp(1).getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTimestamp(2).getTime == DateHelper("2020-08-19T21:33:14.000Z").getTime)
        assert(cs2.getTimestamp(3).getTime == DateHelper("2020-08-19T21:33:07.000Z").getTime)
        // verify by name
        assert(cs2.getDate("avgTime").getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTime("avgTime").getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTimestamp("avgTime").getTime == DateHelper("2020-08-19T21:33:10.750Z").getTime)
        assert(cs2.getTimestamp("maxTime").getTime == DateHelper("2020-08-19T21:33:14.000Z").getTime)
        assert(cs2.getTimestamp("minTime").getTime == DateHelper("2020-08-19T21:33:07.000Z").getTime)
        assert(!cs2.wasNull())
      }
    }

    it("should support a procedure with CLOB parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val (tableName, procName) = getTableAndProcedureNames('D')
        val testFile = "JDBCTestServer.scala"

        // create the table
        val created1 = conn.createStatement().execute(
          s"""|drop if exists $tableName
              |create table $tableName (
              |   file_id: RowNumber,
              |   file_name: String(32),
              |   file_contents: CLOB
              |)
              |""".stripMargin)
        assert(created1)

        // create a reusable statement
        val ps0 = conn.prepareStatement(
          s"""|insert into $tableName (file_name, file_contents)
              |values (?, ?)
              |""".stripMargin)

        // insert data
        files.foreach { file =>
          logger.info(s"including '${file.getName}' (${file.length()} bytes)...")
          ps0.setString(1, file.getName)
          ps0.setClob(2, CLOB.fromFile(file))
          ps0.addBatch()
        }
        val counts = ps0.executeBatch()
        assert(counts.length == files.length && counts.forall(_ == 1))

        // create a reusable statement
        val created2 = conn.createStatement().execute(
          s"""|drop if exists $procName
              |create procedure $procName(theFile: String,
              |                           --> file_id: Int,
              |                           --> file_name: String,
              |                           --> file_contents: CLOB) := {
              |    select file_id, file_name, file_contents
              |    from $tableName
              |    where file_name is theFile
              |}
              |""".stripMargin)
        assert(created2)

        // retrieve a record
        val cs1 = conn.prepareCall(
          s"""|call $procName(?)
              |""".stripMargin)
        cs1.setString(1, testFile)
        cs1.registerOutParameter("file_id", java.sql.Types.INTEGER)
        cs1.registerOutParameter("file_name", java.sql.Types.VARCHAR)
        cs1.registerOutParameter("file_contents", java.sql.Types.CLOB)
        cs1.execute()

        // load the verification file
        val contents = Source.fromFile(files.find(_.getName == testFile) || die(s"File '$testFile' not found")).use(_.mkString).trim

        // verify by index
        assert(cs1.getRowId(1) == JDBCRowId(10))
        assert(cs1.getString(2) == testFile)
        assert(cs1.getBlob(3).getBinaryStream.mkString() == contents)
        assert(new String(cs1.getBytes(3)).trim == contents)
        assert(cs1.getCharacterStream(3).mkString() == contents)
        assert(cs1.getClob(3).getCharacterStream.mkString() == contents)
        assert(cs1.getNCharacterStream(3).mkString() == contents)
        assert(cs1.getNClob(3).getCharacterStream.mkString() == contents)
        assert(cs1.getSQLXML(3).getCharacterStream.mkString() == contents)
        // verify by name
        assert(cs1.getRowId("file_id") == JDBCRowId(10))
        assert(cs1.getString("file_name") == testFile)
        assert(cs1.getBlob("file_contents").getBinaryStream.mkString() == contents)
        assert(new String(cs1.getBytes("file_contents")).trim == contents)
        assert(cs1.getCharacterStream("file_contents").mkString() == contents)
        assert(cs1.getClob("file_contents").getCharacterStream.mkString() == contents)
        assert(cs1.getNCharacterStream("file_contents").mkString() == contents)
        assert(cs1.getNClob("file_contents").getCharacterStream.mkString() == contents)
        assert(cs1.getSQLXML("file_contents").getCharacterStream.mkString() == contents)
      }
    }

  }

  private def getTableAndProcedureNames(suffix: Char): (String, String) = {
    val tableName = getTestTableName + s"_$suffix"
    val procName = s"get$tableName"
    (tableName, procName)
  }

}