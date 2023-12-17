package com.lollypop.database.jdbc

import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.CLOB
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

import java.io.{File, FileInputStream, FileReader}
import java.sql.DriverManager
import scala.io.Source

class JDBCPreparedStatementTest extends AnyFunSpec with JDBCTestServer with VerificationTools {
  private val tableName = getTestTableName
  private val blobTableName = "temp.jdbc.JDBCBlobTest"
  private val clobTableName = "temp.jdbc.JDBCClobTest"
  private val testDir = new File("./app/jdbc-driver/src/test/scala/")
  private val files = testDir.streamFilesRecursively.filter(_.getName.endsWith(".scala"))

  describe(classOf[JDBCPreparedStatement].getSimpleName) {

    it("should create a parameterized table") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create the table
        val ps = conn.prepareStatement(
          s"""|drop if exists $tableName
              |create table if not exists $tableName (
              |  symbol: String(?),
              |  exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
              |  lastSale: Double,
              |  lastSaleTime: DateTime
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
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values (?, ?, ?, ?)
              |""".stripMargin)
        Seq(("ABC", "OTCBB", 1.4285, DateHelper.ts("2023-06-25T21:18:47.016Z"))) foreach {
          case (symbol, exchange, lastSale, lastSaleTime) =>
            ps.setString(1, symbol)
            ps.setString(2, exchange)
            ps.setDouble(3, lastSale)
            ps.setTimestamp(4, lastSaleTime)
        }

        // execute and verify the insert count
        val count = ps.executeUpdate()
        assert(count == 1)
      }
    }

    it("should insert a BATCH of parameterized records") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // insert some records
        val ps = conn.prepareStatement(
          s"""|insert into $tableName (symbol, exchange, lastSale, lastSaleTime)
              |values (?, ?, ?, ?)
              |""".stripMargin)
        Seq(("MSFT", "NYSE", 56.55, DateHelper.ts("2023-06-25T21:18:47.016Z")),
          ("AAPL", "NASDAQ", 98.55, DateHelper.ts("2023-06-25T21:18:47.016Z")),
          ("AMZN", "NYSE", 56.55, DateHelper.ts("2023-06-25T21:18:47.016Z")),
          ("GOOG", "NASDAQ", 98.55, DateHelper.ts("2023-06-25T21:18:47.016Z"))) foreach {
          case (symbol, exchange, lastSale, lastSaleTime) =>
            ps.setString(1, symbol)
            ps.setString(2, exchange)
            ps.setDouble(3, lastSale)
            ps.setTimestamp(4, lastSaleTime)
            ps.addBatch()
        }

        // execute and verify the insert count
        val count = ps.executeBatch()
        assert(count.length == 4 && count.forall(_ == 1))
      }
    }

    it("should verify the inserted data") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|select * from $tableName
              |where exchange is ?
              |""".stripMargin)
        ps.setString(1, "NYSE")
        val rs = ps.executeQuery()
        // AMEX = 0, NASDAQ = 1, NYSE = 2, OTCBB = 3, OTHEROTC = 4
        assert(rs.next())
        assert(rs.getString("symbol") == "MSFT")
        assert(rs.getShort("exchange") == 2)
        assert(rs.getDouble("lastSale") == 56.55)
        assert(rs.getTimestamp("lastSaleTime") == DateHelper.ts("2023-06-25T21:18:47.016Z"))
        assert(rs.next())
        assert(rs.getString("symbol") == "AMZN")
        assert(rs.getShort("exchange") == 2)
        assert(rs.getDouble("lastSale") == 56.55)
        assert(rs.getTimestamp("lastSaleTime") == DateHelper.ts("2023-06-25T21:18:47.016Z"))
        assert(!rs.next())
      }
    }

    it("should execute an update statement") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|update $tableName set lastSale = ?, lastSaleTime = ?
              |where symbol is ?
              |""".stripMargin)
        ps.setDouble(1, 101.12)
        ps.setTimestamp(2, new java.sql.Timestamp(DateHelper("2023-06-25T21:19:47.016Z").getTime))
        ps.setString(3, "GOOG")
        val count = ps.executeUpdate()
        assert(count == 1)
      }
    }

    it("should execute a select query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|select * from $tableName
              |where symbol is ?
              |""".stripMargin)
        ps.setString(1, "AAPL")
        val rs = ps.executeQuery()
        assert(rs.next())
        assert(rs.getString("symbol") == "AAPL")
        assert(rs.getShort("exchange") == 1)
        assert(rs.getDouble("lastSale") == 98.55)
        assert(rs.getTimestamp("lastSaleTime") == DateHelper.ts("2023-06-25T21:18:47.016Z"))
      }
    }

    it("should execute a select query with parameters") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.prepareStatement(
          s"""|select * from $tableName
              |where symbol is ?
              |""".stripMargin) use { ps =>
          ps.setString(1, "GOOG")
          ps.executeQuery() use { rs =>
            assert(rs.next())
            assert(rs.getString("symbol") == "GOOG")
            assert(rs.getShort("exchange") == 1)
            assert(rs.getDouble("lastSale") == 101.12)
            assert(rs.getTimestamp("lastSaleTime") == DateHelper.ts("2023-06-25T21:19:47.016Z"))
          }
        }
      }
    }

    it("should execute a count query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.prepareStatement(
          s"""|select transactions: count(*) from $tableName
              |where exchange in ?
              |""".stripMargin) use { ps =>
          ps.setObject(1, Array("NASDAQ", "NYSE"))
          ps.executeQuery() use { rs =>
            assert(rs.next())
            assert(rs.getLong("transactions") == 4)
          }
        }
      }
    }

    it("should execute a summarization query") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.prepareStatement(
          s"""|select
              |   transactions: count(*),
              |   avgLastSale: avg(lastSale),
              |   minLastSale: min(lastSale),
              |   maxLastSale: max(lastSale),
              |   sumLastSale: sum(lastSale)
              |from $tableName
              |where exchange in ?
              |""".stripMargin) use { ps =>
          ps.setObject(1, Array("NASDAQ", "NYSE"))
          ps.executeQuery() use { rs =>
            assert(rs.next())
            assert(rs.getDouble("sumLastSale") == 312.77)
            assert(rs.getDouble("maxLastSale") == 101.12)
            assert(rs.getDouble("avgLastSale") == 78.1925)
            assert(rs.getDouble("minLastSale") == 56.55)
            assert(rs.getLong("transactions") == 4)
          }
        }
      }
    }

    it("should modify and insert a new record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        conn.prepareStatement(
          s"""|select * from $tableName
              |where symbol is ?
              |""".stripMargin) use { ps =>
          ps.setString(1, "GOOG")
          ps.executeQuery() use { rs0 =>
            assert(rs0.next())
            rs0.updateString("symbol", "CCC")
            rs0.updateDouble("lastSale", 15.44)
            rs0.insertRow()
          }
        }

        // retrieve the row again
        conn.prepareStatement(
          s"""|select * from $tableName
              |where symbol is ?
              |""".stripMargin) use { ps =>
          ps.setString(1, "CCC")
          ps.executeQuery() use { rs1 =>
            assert(rs1.next())
            assert(rs1.getString("symbol") == "CCC")
            assert(rs1.getDouble("lastSale") == 15.44)
          }
        }
      }
    }

    it("should modify and update an existing record via the ResultSet") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val ps = conn.prepareStatement(
          s"""|select * from $tableName
              |where symbol is ?
              |""".stripMargin)

        // query the record for 'GOOG'
        ps.setString(1, "GOOG")
        ps.executeQuery() use { rs =>
          assert(rs.next())

          // update the retrieved record
          rs.updateString("symbol", "AAA")
          rs.updateDouble("lastSale", 78.99)
          rs.updateTimestamp("lastSaleTime", DateHelper.ts("2023-06-25T21:19:53.543Z"))
          rs.updateRow()
          assert(rs.rowUpdated())
        }

        // retrieve the row again
        ps.setString(1, "AAA")
        ps.executeQuery() use { rs1 =>
          assert(rs1.next())
          assert(rs1.getString("symbol") == "AAA")
          assert(rs1.getDouble("lastSale") == 78.99)
          assert(rs1.getTimestamp("lastSaleTime").getTime == DateHelper("2023-06-25T14:19:53.543Z").getTime)
        }
      }
    }

    it("should create a table and insert BLOB data") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create the table
        val created = conn.createStatement().execute(
          s"""|drop if exists $blobTableName
              |create table $blobTableName (
              |   file_id: RowNumber,
              |   file_name: String(64),
              |   file_path: String(256),
              |   file_contents: BLOB
              |)
              |""".stripMargin)
        assert(created)

        // create a reusable statement
        val ps0 = conn.prepareStatement(
          s"""|insert into $blobTableName (file_name, file_path, file_contents)
              |values (?, ?, ?)
              |""".stripMargin)

        // insert data
        files.zipWithIndex.foreach { case (file, n) =>
          logger.info(s"including '${file.getName}' (${file.length()} bytes)...")
          ps0.setString(1, file.getName)
          ps0.setString(2, file.getPath)
          n % 2 match {
            case 0 => ps0.setBinaryStream(3, new FileInputStream(file))
            case 1 => ps0.setBlob(3, new FileInputStream(file))
          }
          ps0.addBatch()
        }
        val counts = ps0.executeBatch()
        assert(counts.length == files.length && counts.forall(_ == 1))

        // create a reusable statement
        val ps1 = conn.prepareStatement(
          s"""|select file_name, file_path, file_contents from $blobTableName where file_name is ?
              |""".stripMargin)

        // retrieve a record
        ps1.setString(1, "JDBCTestServer.scala")
        val rs = ps1.executeQuery()
        assert(!rs.wasNull())

        // verify the retrieved record
        assert(rs.next())
        assert(rs.getString("file_name") == "JDBCTestServer.scala")
        val path = rs.getString("file_path")
        val fromJDBC = rs.getBinaryStream("file_contents").use(_.mkString())
        val fromFile = Source.fromFile(new File(path)).use(_.mkString)
        assert(fromJDBC.trim == fromFile.trim)
        assert(!rs.next())
      }
    }

    it("should create a table and insert CLOB data") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create the table
        val created = conn.createStatement().execute(
          s"""|drop if exists $clobTableName
              |create table $clobTableName (
              |   file_id: RowNumber,
              |   file_name: String(64),
              |   file_path: String(256),
              |   file_contents: CLOB
              |)
              |""".stripMargin)
        assert(created)

        // create a reusable statement
        val ps0 = conn.prepareStatement(
          s"""|insert into $clobTableName (file_name, file_path, file_contents)
              |values (?, ?, ?)
              |""".stripMargin)

        // insert data
        files.zipWithIndex.foreach { case (file, n) =>
          logger.info(s"including '${file.getName}' (${file.length()} bytes)...")
          ps0.setString(1, file.getName)
          ps0.setString(2, file.getPath)
          n % 5 match {
            case 0 => ps0.setAsciiStream(3, new FileInputStream(file))
            case 1 => ps0.setCharacterStream(3, new FileReader(file))
            case 2 => ps0.setClob(3, new FileReader(file))
            case 3 => ps0.setNCharacterStream(3, new FileReader(file))
            case 4 => ps0.setNClob(3, new FileReader(file))
          }
          ps0.addBatch()
        }
        val counts = ps0.executeBatch()
        assert(counts.length == files.length && counts.forall(_ == 1))

        // create a reusable statement
        val ps1 = conn.prepareStatement(
          s"""|select file_name, file_path, file_contents from $clobTableName where file_name is ?
              |""".stripMargin)

        // retrieve a record
        ps1.setString(1, "JDBCTestServer.scala")
        val rs = ps1.executeQuery()

        // verify the retrieved record
        assert(rs.next())
        assert(rs.getString("file_name") == "JDBCTestServer.scala")
        val path = rs.getString("file_path")
        val fromJDBC = rs.getCharacterStream("file_contents").mkString()
        val fromFile = Source.fromFile(new File(path)).use(_.mkString)
        assert(fromJDBC.trim == fromFile.trim)
        assert(!rs.next())
      }
    }

    it("should create a table and insert BLOB data with lengths") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create the table
        val created = conn.createStatement().execute(
          s"""|drop if exists $blobTableName
              |create table $blobTableName (
              |   file_id: RowNumber,
              |   file_name: String(64),
              |   file_path: String(256),
              |   file_contents: BLOB
              |)
              |""".stripMargin)
        assert(created)

        // create a reusable statement
        val ps0 = conn.prepareStatement(
          s"""|insert into $blobTableName (file_name, file_path, file_contents)
              |values (?, ?, ?)
              |""".stripMargin)

        // insert data
        files.zipWithIndex.foreach { case (file, n) =>
          logger.info(s"including '${file.getName}' (${file.length()} bytes)...")
          ps0.setString(1, file.getName)
          ps0.setString(2, file.getPath)
          n % 2 match {
            case 0 => ps0.setBinaryStream(3, new FileInputStream(file), file.length())
            case 1 => ps0.setBlob(3, new FileInputStream(file), file.length())
          }
          ps0.addBatch()
        }
        val counts = ps0.executeBatch()
        assert(counts.length == files.length && counts.forall(_ == 1))

        // create a reusable statement
        val ps1 = conn.prepareStatement(
          s"""|select file_name, file_path, file_contents from $blobTableName where file_name is ?
              |""".stripMargin)

        // retrieve a record
        ps1.setString(1, "JDBCTestServer.scala")
        val rs = ps1.executeQuery()

        // verify the retrieved record
        assert(rs.next())
        assert(rs.getString("file_name") == "JDBCTestServer.scala")
        val path = rs.getString("file_path")
        val fromJDBC = rs.getBlob("file_contents").getBinaryStream.use(_.mkString())
        val fromFile = Source.fromFile(new File(path)).use(_.mkString)
        assert(fromJDBC.trim == fromFile.trim)
        assert(!rs.next())
      }
    }

    it("should create a table and insert CLOB data with lengths") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // create the table
        val created = conn.createStatement().execute(
          s"""|drop if exists $clobTableName
              |create table $clobTableName (
              |   file_id: RowNumber,
              |   file_name: String(64),
              |   file_path: String(256),
              |   file_contents: CLOB
              |)
              |""".stripMargin)
        assert(created)

        // create a reusable statement
        val ps0 = conn.prepareStatement(
          s"""|insert into $clobTableName (file_name, file_path, file_contents)
              |values (?, ?, ?)
              |""".stripMargin)

        // insert data
        files.zipWithIndex.foreach { case (file, n) =>
          logger.info(s"including '${file.getName}' (${file.length()} bytes)...")
          ps0.setString(1, file.getName)
          ps0.setString(2, file.getPath)
          n % 6 match {
            case 0 => ps0.setAsciiStream(3, new FileInputStream(file), file.length())
            case 1 => ps0.setCharacterStream(3, new FileReader(file), file.length())
            case 2 => ps0.setClob(3, CLOB.fromReader(new FileReader(file), file.length()))
            case 3 => ps0.setClob(3, new FileReader(file), file.length())
            case 4 => ps0.setNCharacterStream(3, new FileReader(file), file.length())
            case 5 => ps0.setNClob(3, new FileReader(file), file.length())
          }
          ps0.addBatch()
        }
        val counts = ps0.executeBatch()
        assert(counts.length == files.length && counts.forall(_ == 1))

        // create a reusable statement
        val ps1 = conn.prepareStatement(
          s"""|select file_name, file_path, file_contents from $clobTableName where file_name is ?
              |""".stripMargin)

        // retrieve a record
        ps1.setString(1, "JDBCTestServer.scala")
        val rs = ps1.executeQuery()

        // verify the retrieved record
        assert(rs.next())
        assert(rs.getString("file_name") == "JDBCTestServer.scala")
        val path = rs.getString("file_path")
        val clob = rs.getClob("file_contents")
        val fromJDBC = clob.getAsciiStream.mkString()
        val fromFile = Source.fromFile(new File(path)).use(_.mkString)
        assert(fromJDBC.trim == fromFile.trim)
        assert(!rs.next())
        clob.free()
      }
    }

  }

}
