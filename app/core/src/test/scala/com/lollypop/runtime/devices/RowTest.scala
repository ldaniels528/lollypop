package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{DateTimeType, Float32Type, StringType}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class RowTest extends AnyFunSpec {

  describe(classOf[Row].getSimpleName) {

    it("should transform a Map to a Row") {
      val row = createTestRow
      assert(row.toMap == Map(
        "symbol" -> "ABC",
        "exchange" -> "NYSE",
        "lastSale" -> 123.45,
        "lastSaleTime" -> DateHelper.from(1656631146144L),
        "comments" -> "The brown fox jumped over the lazy dog"))
    }

    it("should facilitate removing a key from a Row") {
      val row = createTestRow
      assert((row - "comments") == Map(
        "symbol" -> "ABC",
        "exchange" -> "NYSE",
        "lastSale" -> 123.45,
        "lastSaleTime" -> DateHelper.from(1656631146144L)))
    }

    it("should facilitate removing multiple keys from a Row") {
      val row = createTestRow
      assert((row - ("comments", "lastSaleTime")) == Map(
        "symbol" -> "ABC",
        "exchange" -> "NYSE",
        "lastSale" -> 123.45))
    }

    it("should provide iteration over a Row") {
      val row = createTestRow
      assert(row.iterator.toList == row.toMap.toList)
    }

  }

  private def createTestRow: Row = {
    val mappings = Map(
      "symbol" -> "ABC",
      "exchange" -> "NYSE",
      "lastSale" -> 123.45,
      "lastSaleTime" -> DateHelper.from(1656631146144L),
      "comments" -> "The brown fox jumped over the lazy dog")

    mappings.toRow(rowID = 100L)(RecordStructure(Seq(
      TableColumn(name = "symbol", `type` = StringType(4)),
      TableColumn(name = "exchange", `type` = StringType(6)),
      TableColumn(name = "lastSale", `type` = Float32Type),
      TableColumn(name = "lastSaleTime", `type` = DateTimeType),
      TableColumn(name = "comments", `type` = StringType(128))
    )))
  }

}
