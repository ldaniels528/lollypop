package com.lollypop.runtime.devices

import com.lollypop.language.models.@@
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{DateTimeType, Float64Type, StringType}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ModelRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[ModelRowCollection].getSimpleName) {

    it("should support inserting rows into a table") {
      assert(createTestTable.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
    }

    it("should support retrieving a row by index: stocks[3]") {
      val (_, _, result) = LollypopVM.executeSQL(Scope().withVariable("stocks", createTestTable),
        """|stocks[3]
           |""".stripMargin)
      val row_? = Option(result).collect { case r: Row => r.toMap }
      assert(row_? contains Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L)))
    }

    it("should support retrieving a field by index: stocks[3][0]") {
      val (_, _, result) = LollypopVM.executeSQL(Scope().withVariable("stocks", createTestTable),
        """|stocks[3][0]
           |""".stripMargin)
      assert(result == "KNOW")
    }

    it("should support updating rows") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope().withVariable("stocks", createTestTable),
        """|update @stocks set lastSale = 358.21 where symbol is 'KNOW'
           |""".stripMargin)
      assert(cost.updated == 1)
      assert(scope.getRowCollection(@@("stocks")).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 358.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
    }

    it("should support deleting rows") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope().withVariable("stocks", createTestTable),
        """|delete from @stocks where symbol is 'KFFQ'
           |""".stripMargin)
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(@@("stocks")).toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
    }

    it("should support deleting rows via metadata") {
      val rc = createTestTable
      rc.updateRowMetadata(rowID = 2L, rmd = RowMetadata(isAllocated = false))
      assert(rc.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
    }

    it("should support push operations") {
      val (_, _, resultA) = LollypopVM.searchSQL(Scope(),
        """|declare table stocks(
           |  symbol: String(4),
           |  exchange: String(6),
           |  lastSale: Double,
           |  lastSaleTime: DateTime
           |)
           |stocks.push({ symbol: 'ABC', exchange: 'OTCBB', lastSale: 37.89, lastSaleTime: DateTime('2022-09-04T23:36:46.862Z') })
           |stocks.push({ symbol: 'T', exchange: 'NYSE', lastSale: 22.77, lastSaleTime: DateTime('2022-09-04T23:36:47.321Z') })
           |stocks.push({ symbol: 'AAPL', exchange: 'NASDAQ', lastSale: 149.76, lastSaleTime: DateTime('2022-09-04T23:36:48.503Z') })
           |stocks.push({ symbol: 'KNOW', exchange: 'NYSE', lastSale: 357.21 })
           |stocks.push({ symbol: 'KJV', exchange: 'OTCBB', lastSale: 16.11, lastSaleTime: DateTime('2022-09-04T23:36:48.503Z') })
           |stocks
           |""".stripMargin)
      resultA.tabulate().foreach(logger.info)
      assert(resultA.toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 37.89, "lastSaleTime" -> DateHelper("2022-09-04T23:36:46.862Z")),
        Map("symbol" -> "T", "exchange" -> "NYSE", "lastSale" -> 22.77, "lastSaleTime" -> DateHelper("2022-09-04T23:36:47.321Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 149.76, "lastSaleTime" -> DateHelper("2022-09-04T23:36:48.503Z")),
        Map("symbol" -> "KNOW", "exchange" -> "NYSE", "lastSale" -> 357.21),
        Map("symbol" -> "KJV", "exchange" -> "OTCBB", "lastSale" -> 16.11, "lastSaleTime" -> DateHelper("2022-09-04T23:36:48.503Z"))
      ))
    }

    it("should return the size of the dataframe in bytes") {
      assert(createTestTable.sizeInBytes == 164L)
    }

    it("should support field update operations") {
      val rc = createTestTable
      val newDateTime = DateHelper("2023-08-02T03:31:12.589Z")
      rc.updateField(rowID = 1L, columnID = 3, newValue = Some(newDateTime))
      assert(rc.readField(rowID = 1L, columnID = 3).value contains newDateTime)
    }

    it("should drop values from rows where the field is inactive") {
      val rc = createTestTable
      rc.updateFieldMetadata(rowID = 1L, columnID = 3, fmd = FieldMetadata(isActive = false))
      assert(rc(1L).toMap == Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92))
    }

    it("should return a field with an empty value when the field is inactive") {
      val rc = createTestTable
      rc.updateFieldMetadata(rowID = 2L, columnID = 3, fmd = FieldMetadata(isActive = false))
      assert(rc.readField(rowID = 2L, columnID = 3).value.isEmpty)
    }

    it("should read metadata reflecting the field is inactive") {
      val rc = createTestTable
      rc.updateFieldMetadata(rowID = 3L, columnID = 3, fmd = FieldMetadata(isActive = false))
      assert(rc.readFieldMetadata(rowID = 3L, columnID = 3).isNull)
    }

  }

  private def createTestTable: ModelRowCollection = {
    val device = ModelRowCollection(columns = Seq(
      TableColumn(name = "symbol", `type` = StringType(6)),
      TableColumn(name = "exchange", `type` = StringType(6)),
      TableColumn(name = "lastSale", `type` = Float64Type),
      TableColumn(name = "lastSaleTime", `type` = DateTimeType)))
    val stocks = List(
      Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
      Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
      Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
      Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L)))
    device.insert(stocks.map(_.toRow(rowID = 0L)(device)))
    device
  }

}
