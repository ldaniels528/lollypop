package com.lollypop.runtime

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.language.models.{Column, TableModel}
import com.lollypop.runtime.DatabaseObjectNS._
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.datatypes.TableType.TableTypeRefExtensions
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import org.scalatest.funspec.AnyFunSpec

class DatabaseObjectNSTest extends AnyFunSpec {

  describe(classOf[DatabaseObjectNS].getSimpleName) {

    it("should determine the file location of an inner table") {
      val ns = DatabaseObjectNS(databaseName = "finance", schemaName = "securities", name = "stocks", columnName = Some("transactions"))
      assert(ns.subTableDataFile contains getDatabaseRootDirectory(ns.databaseName) / ns.schemaName / ns.name / (ns.name + "#" + ns.columnName.orNull + "." + dataExt))
    }

    it("should determine the file location of a database object config file") {
      val ns = DatabaseObjectNS(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest")
      assert(ns.configFile == getDatabaseRootDirectory(ns.databaseName) / ns.schemaName / ns.name / (ns.name + "." + configExt))
    }

    it("should determine the file location of a database object BLOB file") {
      val ns = DatabaseObjectNS(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest")
      assert(ns.blobDataFile == getDatabaseRootDirectory(ns.databaseName) / ns.schemaName / ns.name / (ns.name + "." + blobDataExt))
    }

    it("should determine the file location of a database object data file") {
      val ns = DatabaseObjectNS(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest")
      assert(ns.tableDataFile == getDatabaseRootDirectory(ns.databaseName) / ns.schemaName / ns.name / (ns.name + "." + dataExt))
    }

    it("should determine the file location of a database object journal file") {
      val ns = DatabaseObjectNS(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest")
      assert(ns.journalFile == getDatabaseRootDirectory(ns.databaseName) / ns.schemaName / ns.name / (ns.name + "." + journalFileExt))
    }

    it("should determine the database object for a namespace") {
      val ns = DatabaseObjectNS(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest")
      assert(ns.getReference == DatabaseObjectRef(databaseName = "unit_test", schemaName = "files", name = "DatabaseFilesTest"))
    }

    it("should retrieve both the config and table for a given namespace") {
      implicit val scope: Scope = Scope()
      val columns = List(
        Column(name = "symbol", `type` = "String".ct),
        Column(name = "exchange", `type` = "String".ct),
        Column(name = "price", `type` = "Double".ct),
        Column(name = "time", `type` = "DateTime".ct)
      )
      val ns = DatabaseObjectNS(databaseName = DEFAULT_DATABASE, schemaName = DEFAULT_SCHEMA, name = getClass.getSimpleName)
      DatabaseManagementSystem.dropObject(ns, ifExists = true)
      DatabaseManagementSystem.createPhysicalTable(ns, TableModel(columns).toTableType, ifNotExists = false)
      val (config, table) = ns.readTableAndConfig
      assert(config.columns == columns.map(_.toTableColumn))
      assert(table.columns == columns.map(_.toTableColumn))
      table.close()
    }

  }

}
