package com.qwery.runtime

import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Database Management System Test
 */
class DatabaseManagementSystemTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[DatabaseManagementSystem].getSimpleName) {

    it("should provide the collection of environment tables") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|from (OS.getEnv()) where name is 'HOME'
           |""".stripMargin
      )
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("name" -> "HOME", "value" -> scala.util.Properties.userHome),
      ))
    }

    it("should create a table for testing") {
      QweryVM.executeSQL(Scope(), sql =
        s"""|drop if exists temp.stocks.stocksDMS
            |create table if not exists temp.runtime.stocksDMS (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double,
            |   lastSaleTime: DateTime
            |)
            |""".stripMargin)
    }

    it("should retrieve the details for a specific table") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|namespace 'temp.runtime'
           |from (OS.getDatabaseObjects()) where name like '%stocksDM%'
           |""".stripMargin
      )
      device.tabulate() foreach logger.info
      assert(device.toMapGraph.map(_.filterNot(t => Seq("lastModifiedTime", SRC_ROWID_NAME).contains(t._1))) == List(
        Map("database" -> "temp", "name" -> "stocksDMS", "schema" -> "runtime", "comment" -> "", "qname" -> "temp.runtime.stocksDMS", "type" -> "table")
      ))
    }

    it("should search for columns from within a specific table") {
      val (_, _, device) = QweryVM.searchSQL(Scope(),
        """|namespace 'temp.runtime'
           |from (OS.getDatabaseColumns()) where name like '%stocksDM%'
           |""".stripMargin
      )
      device.tabulate(limit = 20).foreach(logger.info)
      assert(device.toMapGraph.map(_.filterNot(t => Seq("lastModifiedTime", SRC_ROWID_NAME).contains(t._1))) == List(
        Map("database" -> "temp", "name" -> "stocksDMS", "columnTypeName" -> "String", "columnName" -> "symbol", "JDBCType" -> 12, "columnType" -> "String(8)", "schema" -> "runtime", "qname" -> "temp.runtime.stocksDMS", "maxSizeInBytes" -> 8, "type" -> "table"),
        Map("database" -> "temp", "name" -> "stocksDMS", "columnTypeName" -> "String", "columnName" -> "exchange", "JDBCType" -> 12, "columnType" -> "String(8)", "schema" -> "runtime", "qname" -> "temp.runtime.stocksDMS", "maxSizeInBytes" -> 8, "type" -> "table"),
        Map("database" -> "temp", "name" -> "stocksDMS", "columnTypeName" -> "Double", "columnName" -> "lastSale", "JDBCType" -> 8, "columnType" -> "Double", "schema" -> "runtime", "qname" -> "temp.runtime.stocksDMS", "maxSizeInBytes" -> 8, "type" -> "table"),
        Map("database" -> "temp", "name" -> "stocksDMS", "columnTypeName" -> "DateTime", "columnName" -> "lastSaleTime", "JDBCType" -> 93, "columnType" -> "DateTime", "schema" -> "runtime", "qname" -> "temp.runtime.stocksDMS", "maxSizeInBytes" -> 8, "type" -> "table")
      ))
    }

  }

}
