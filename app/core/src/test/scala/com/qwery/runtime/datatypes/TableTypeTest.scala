package com.qwery.runtime.datatypes

import com.qwery.language.QweryUniverse
import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.util.DateHelper

/**
 * TableType Tests
 */
class TableTypeTest extends DataTypeFunSpec {
  implicit val ctx: QweryUniverse = QweryUniverse()
  implicit val scope: Scope = Scope()

  describe(TableType.getClass.getSimpleName) {

    it("should resolve 'Table ( remarks String(255), createdDate DateTime )[100]'") {
      verifySpec(spec = "Table ( remarks String(255), createdDate DateTime )[100]", expected = TableType(columns = Seq(
        Column(name = "remarks", `type` = ColumnType("String", size = 255)),
        Column(name = "createdDate", `type` = ColumnType("DateTime"))
      ).map(_.toTableColumn), capacity = 100))
    }

    it("should resolve a JSON array as a Table Device") {
      val jsonString =
        """|[
           |  {"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
           |  {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}
           |]""".stripMargin
      val tableType = TableType(capacity = 2, columns = Seq(
        Column(name = "price", `type` = ColumnType("Double")),
        Column(name = "transactionTime", `type` = ColumnType("DateTime"))).map(_.toTableColumn))
      val device = tableType.fromJSON(jsonString)
      device.foreach { row => logger.info(s"row ${row.toMap}") }
      assert(device.toMapGraph == List(
        Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
        Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
      ))
    }

    it("should resolve a JSON object as a Table Device") {
      val jsonString = """{"price":65.00, "transactionTime":"2021-08-05T19:23:11.000Z"}"""
      val tableType = TableType(capacity = 1, columns = Seq(
        Column(name = "price", `type` = ColumnType("Double")),
        Column(name = "transactionTime", `type` = ColumnType("DateTime"))).map(_.toTableColumn))
      val device = tableType.fromJSON(jsonString)
      device.foreach { row => logger.info(s"row ${row.toMap}") }
      assert(device.toMapGraph == List(
        Map("price" -> 65.0, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
      ))
    }

    it("should provide a SQL representation: Table(remarks String(255),createdDate DateTime)[100]") {
      verifySQL(
        sql = "Table(remarks: String(255), createdDate: DateTime)[100]",
        dataType = TableType(columns = Seq(
          Column(name = "remarks", `type` = ColumnType("String", size = 255)),
          Column(name = "createdDate", `type` = ColumnType("DateTime"))
        ).map(_.toTableColumn), capacity = 100)
      )
    }

    it("should provide a SQL representation: Table(remarks String(255),createdDate DateTime)[100]*") {
      verifySQL(
        sql = "Table(remarks: String(255), createdDate: DateTime)[100]*",
        dataType = TableType(
          capacity = 100,
          isPointer = true,
          columns = Seq(
            Column(name = "remarks", `type` = ColumnType("String", size = 255)),
            Column(name = "createdDate", `type` = ColumnType("DateTime"))
          ).map(_.toTableColumn))
      )
    }

  }

}
