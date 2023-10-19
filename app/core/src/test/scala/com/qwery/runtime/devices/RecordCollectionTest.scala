package com.qwery.runtime.devices

import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.Scope
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import org.scalatest.funspec.AnyFunSpec
import qwery.io.IOCost

class RecordCollectionTest extends AnyFunSpec {
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

  describe(classOf[RecordCollection[_]].getSimpleName) {

    it("should use .apply() to retrieve one row by ID") {
      assert(newTable().apply(0).toMap == Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L))
    }

    it("should use .compact() to remove dead entries from of the collection") {
      val device = newTable()
      assert(device.delete(rowID = 2) ++ device.delete(rowID = 3) == IOCost(deleted = 2))
      assert(device.compact() == IOCost(deleted = 1, scanned = 1))
      assert(device.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L)
      ))
    }

    it("should use .deleteSlice() to remove a contiguous range of rows from of the collection") {
      val device = newTable()
      assert(device.deleteSlice(rowID0 = 2, rowID1 = 3) == IOCost(deleted = 2))
      assert(device.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L)
      ))
    }

    it("should use .reverseInPlace() to reverse the collection (in place)") {
      val device = newTable()
      assert(device.reverseInPlace() == IOCost(scanned = 4, updated = 4))
      assert(device.toMapGraph == List(
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L)
      ))
    }

    it("should use .swap() to swap the position of two rows") {
      val device = newTable()
      assert(device.swap(rowID0 = 0, rowID1 = 3) == IOCost(scanned = 2, updated = 2))
      assert(device.toMapGraph == List(
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L)
      ))
    }

  }

  private def newTable(): RowCollection = {
    implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
    stocks.foreach(row => device.insert(row.toRow))
    device
  }

}
