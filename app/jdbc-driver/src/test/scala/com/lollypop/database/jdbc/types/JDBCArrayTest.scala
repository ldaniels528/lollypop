package com.lollypop.database.jdbc.types

import com.lollypop.database.jdbc.{JDBCTestServer, RichResult}
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec

import java.sql.DriverManager

class JDBCArrayTest extends AnyFunSpec with JDBCTestServer {
  val tableName = "temp.jdbc.JDBCArrayTest"

  describe(classOf[JDBCArray].getSimpleName) {

    it("should represent an Array") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val array = conn.createArrayOf("String", Array("Larry", "Curly", "Moe"))
        assert(Option(array.getArray).collect { case a: Array[_] => a.toSeq } contains Seq("Larry", "Curly", "Moe"))
        array.free()
      }
    }

    it("should return the JDBC base type of the Array") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val array = conn.createArrayOf("String", Array("Larry", "Curly", "Moe"))
        assert(array.getBaseType == java.sql.Types.VARCHAR)
        array.free()
      }
    }

    it("should return the JDBC base type name of the Array") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val array = conn.createArrayOf("String", Array("Larry", "Curly", "Moe"))
        assert(array.getBaseTypeName == "String")
        array.free()
      }
    }

    it("should produce a result set from an Array") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val array = conn.createArrayOf("String", Array("Larry", "Curly", "Moe"))
        val rc = array.getResultSet.toRowCollection
        assert(rc.toMapGraph == List(Map("item" -> "Larry"), Map("item" -> "Curly"), Map("item" -> "Moe")))
        array.free()
      }
    }

    it("should execute a create table with ARRAY data") {
      DriverManager.getConnection(jdbcURL) use { conn =>
        val created = conn.createStatement().execute(
          s"""|drop if exists $tableName
              |create table $tableName (region_name: String(32), zip_codes: String[10] = [])
              |""".stripMargin)
        assert(created)

        // insert some rows
        val ps = conn.prepareStatement(
          s"""|insert into $tableName (region_name, zip_codes) values (?, ?)
              |""".stripMargin)
        ps.setString(1, "CA")
        ps.setArray(2, conn.createArrayOf("String", Array("90064", "90210", "92612")))
        ps.executeUpdate()

        // query the results
        val rs = conn.createStatement().executeQuery(
          s"""|select * from $tableName
              |""".stripMargin)
        val rc = rs.toRowCollection
        assert(rc.toMapGraph.map(_.map {
          case (k@"zip_codes", v: Array[_]) => k -> v.toSeq
          case (k, v) => k -> v
        }) == List(Map(
          "region_name" -> "CA", "zip_codes" -> Seq("90064", "90210", "92612"))
        ))
      }
    }

  }

}
