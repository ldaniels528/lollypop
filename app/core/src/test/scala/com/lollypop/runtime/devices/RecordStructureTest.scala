package com.lollypop.runtime.devices

import com.lollypop.runtime.datatypes.{DateTimeType, Float32Type, StringType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

/**
 * Record Structure Test Suite
 */
class RecordStructureTest extends AnyFunSpec {

  describe(classOf[RecordStructure].getSimpleName) {

    it("should process fixed-length records") {
      // define the fixed-length record structure
      val structure = RecordStructure(Seq(
        TableColumn(name = "symbol", `type` = StringType(4)),
        TableColumn(name = "exchange", `type` = StringType(6)),
        TableColumn(name = "lastSale", `type` = Float32Type),
        TableColumn(name = "lastSaleTime", `type` = DateTimeType),
        TableColumn(name = "comments", `type` = StringType(40))
      ))

      // verify the record structure
      assert(structure.recordSize == 80)
      assert(structure.columnOffsets == Seq(1, 10, 21, 26, 35))
      assert(structure.columns.forall(!_.isExternal))

      // define a row
      val mappings = Map(
        "symbol" -> "ABC",
        "exchange" -> "OTCBB",
        "lastSale" -> 123.45,
        "lastSaleTime" -> DateHelper.from(1656631146144L),
        "comments" -> "The brown fox jumped over the lazy dog")
      val row = mappings.toRow(rowID = 54321L)(structure)

      // verify the row of data
      assert(row.id == 54321L)
      assert(row.toMap == mappings)
//      assert(structure.toFieldBuffers(row.fields).map(_.array()) ==
//        row.columns.zip(row.fields).map { case (col, fld) => col.`type`.encodeFull(fld.value).array()})
      assert(Map(row.fields.map(f => f.name -> f.value.orNull): _*) == mappings)
    }

    it("should process variable-length records") {
      // define the variable-length record structure
      val structure = RecordStructure(Seq(
        TableColumn(name = "symbol", `type` = StringType(4)),
        TableColumn(name = "exchange", `type` = StringType(6)),
        TableColumn(name = "lastSale", `type` = Float32Type),
        TableColumn(name = "lastSaleTime", `type` = DateTimeType),
        TableColumn(name = "comments", `type` = StringType)
      ))

      // verify the record structure
      assert(structure.recordSize == 296)
      assert(structure.columnOffsets == Seq(1, 10, 21, 26, 35))
      assert(!structure.columns.forall(_.isExternal))

      // define a row
      val mappings = Map(
        "symbol" -> "ABC",
        "exchange" -> "OTCBB",
        "lastSale" -> 123.45,
        "lastSaleTime" -> DateHelper.from(1656631146144L),
        "comments" -> "The brown fox jumped over the lazy dog")
      val row = mappings.toRow(rowID = 1234L)(structure)

      // verify the row of data
      assert(row.id == 1234L)
      assert(row.toMap == mappings)
      assert(Map(row.fields.map(f => f.name -> f.value.orNull): _*) == mappings)
    }

  }

}
