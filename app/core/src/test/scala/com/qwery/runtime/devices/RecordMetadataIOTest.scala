package com.qwery.runtime.devices

import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo.createTempTable
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.util.ResourceHelper.AutoClose
import org.scalatest.funspec.AnyFunSpec
import qwery.io.IOCost

/**
 * Record Metadata I/O Test Suite
 * @author lawrence.daniels@gmail.com
 */
class RecordMetadataIOTest extends AnyFunSpec {
  implicit val scope: Scope = Scope()
  private val columns = Seq(
    Column(name = "symbol", `type` = ColumnType("String", size = 8)),
    Column(name = "exchange", `type` = ColumnType("String", size = 8)),
    Column(name = "lastSale", `type` = ColumnType("Double")),
    Column(name = "lastSaleTime", `type` = ColumnType("Long"))
  ).map(_.toTableColumn)
  private val stocks = List(
    Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
    Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
    Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
    Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
  )

  describe(classOf[RecordMetadataIO].getSimpleName) {

    it("should use .countMetadata() to count the rows where: isActive is true") {
      assert(newTable().countWhereMetadata(_.isActive) == stocks.length)
    }

    it("should use .delete() to remove records by its offset (rowID)") {
      assert(newTable().delete(2) == IOCost(deleted = 1))
    }

    it("should use .deleteField() to remove individual field values") {
      val device = newTable()
      assert(device.deleteField(rowID = 0, columnID = 0) == IOCost(deleted = 1))
      assert(device.readField(rowID = 0, columnID = 0).value.isEmpty)
    }

    it("should use .getLength() to retrieve the watermarked size of the device") {
      assert(newTable().getLength == 4)
    }

    it("should use .getRowSummary() to retrieve the row statistics") {
      val device = newTable()
      assert(device.getRowSummary == RowSummary(active = 4))
      device.delete(0)
      assert(device.getRowSummary == RowSummary(active = 3, deleted = 1))
    }

    it("should use .isEmpty() and .nonEmpty() to if the table is empty or non-empty respectively") {
      val device = newTable()
      assert(device.nonEmpty)
      device.setLength(0)
      assert(device.isEmpty)
    }

    it("should use .isMemoryResident() to determine whether a collection resides in RAM") {
      assert(newTable().isMemoryResident)
      assert(!createTempTable(columns).use(_.isMemoryResident))
    }

    it("should use .indexWhereMetadata() to determine the first row ID where a row meets specific metadata criterion") {
      val device = newTable()
      assert(device.delete(0) ++ device.delete(3) == IOCost(deleted = 2))
      assert(device.indexWhereMetadata()(_.isDeleted) contains 0)
    }

    it("should use .lastIndexWhereMetadata() to determine the last row ID where a row meets specific metadata criterion") {
      val device = newTable()
      assert(device.delete(0) ++ device.delete(3) == IOCost(deleted = 2))
      assert(device.lastIndexWhereMetadata()(_.isDeleted) contains 3)
    }

    it("should use .readField() to read individual field values") {
      val device = newTable()
      assert(device.readField(rowID = 0, columnID = 0).value contains "BXXG")
      assert(device.readField(rowID = 1, columnID = 1).value contains "NYSE")
      assert(device.readField(rowID = 2, columnID = 2).value contains 240.14)
      assert(device.readField(rowID = 3, columnID = 3).value contains 1597872791000L)
    }

    it("should use .readFieldMetadata() to retrieve field metadata by rowID and columnID") {
      assert(newTable().readFieldMetadata(rowID = 1, columnID = 3) == FieldMetadata())
    }

    it("should use .readRowMetadata() to retrieve row metadata by rowID") {
      assert(newTable().readRowMetadata(1) == RowMetadata())
    }

    it("should use .recordSize() to retrieve the record size of the table") {
      assert(newTable().recordSize == 45)
    }

    it("should use .setLength() to truncate the collection") {
      val device = newTable()
      device.setLength(2)
      assert(device.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L)
      ))
    }

    it("should use .undelete() to restore a previously deleted row") {
      val device = newTable()
      assert(device.delete(1) ++ device.delete(2) == IOCost(deleted = 2))
      assert(device.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
      assert(device.undelete(2) == IOCost(updated = 1))
      assert(device.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

  }

  private def newTable(): RowCollection = {
    implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
    stocks.foreach(row => device.insert(row.toRow))
    device
  }

}
