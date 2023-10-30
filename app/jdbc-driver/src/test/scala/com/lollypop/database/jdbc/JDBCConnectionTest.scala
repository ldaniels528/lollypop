package com.lollypop.database.jdbc

import com.lollypop.database.jdbc.types.JDBCStruct
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.sql.{Connection, DriverManager}

class JDBCConnectionTest extends AnyFunSpec with JDBCTestServer {

  describe(classOf[JDBCStruct].getSimpleName) {

    it("should indicate whether the connection is closed") {
      val conn = DriverManager.getConnection(jdbcURL)
      assert(!conn.isClosed)
      conn.close()
      assert(conn.isClosed)
    }

    it("should create BLOB objects") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val blob = conn.createBlob()
        blob.setBytes(0L, "Hello World".getBytes())
        assert(blob.length() == 11)
        blob.free()
      }
    }

    it("should create CLOB objects") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val clob = conn.createClob()
        clob.setString(0L, "Hello World")
        assert(clob.length() == 11)
        clob.free()
      }
    }

    it("should create NCLOB objects") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val clob = conn.createNClob()
        clob.setString(0L, "Hello World")
        assert(clob.length() == 11)
        clob.free()
      }
    }

    it("should create SQLXML objects") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val sqlXml = conn.createSQLXML()
        sqlXml.setString("Hello World")
        assert(sqlXml.getString == "Hello World")
        sqlXml.free()
      }
    }

    it("should determine the status of transactionIsolation") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        assert(conn.getTransactionIsolation == Connection.TRANSACTION_NONE)
      }
    }

    it("should create and rollback a save point") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        // ensure we're not in a transaction
        assert(conn.getAutoCommit)

        // create a save point
        val savePoint = conn.setSavepoint("1st")
        assert(!conn.getAutoCommit)

        // rollback the save point
        conn.rollback(savePoint)

        // release the save point
        conn.releaseSavepoint(savePoint)

        // restore auto-commit
        conn.setAutoCommit(true)
      }
    }

  }

}
