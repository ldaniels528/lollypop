package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.runtime.devices.errors.{UniqueKeyAlreadyExistsError, UniqueKeyCannotBeDisabledError}
import com.lollypop.runtime.devices.{FieldMetadata, HashIndexRowCollection, RowMetadata}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import lollypop.io.{IOCost, RowIDRange}

class CreateUniqueIndexTest extends AnyFunSpec with VerificationTools {
  private implicit val compiler: LollypopCompiler = LollypopCompiler()
  private val tableRef = DatabaseObjectRef(getTestTableName)

  describe(classOf[CreateUniqueIndex].getSimpleName) {

    it("should compile: create unique index stocks#symbol") {
      val results = compiler.compile(
        """|create unique index stocks#symbol
           |""".stripMargin)
      assert(results == CreateUniqueIndex(ref = DatabaseObjectRef("stocks#symbol"), ifNotExists = false))
    }

    it("should compile: create unique index if not exists com.acme.roadrunner#trap") {
      val results = compiler.compile(
        """|create unique index if not exists com.acme.roadrunner#trap
           |""".stripMargin)
      assert(results == CreateUniqueIndex(ref = DatabaseObjectRef("com.acme.roadrunner#trap"), ifNotExists = true))
    }

    it("should decompile: create unique index stocks#symbol") {
      verify(
        """|create unique index stocks#symbol
           |""".stripMargin)
    }

    it("should decompile: create unique index if not exists com.acme.roadrunner#trap") {
      verify(
        """|create unique index if not exists com.acme.roadrunner#trap
           |""".stripMargin)
    }

  }

  describe(classOf[HashIndexRowCollection].getSimpleName) {

    it("should create a new table with a unique index") {
      val (_, cost0, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $tableRef &&
            |create table $tableRef (
            |   symbol: String(5),
            |   exchange: String(6),
            |   lastSale: Double
            |) &&
            |create unique index $tableRef#symbol &&
            |insert into $tableRef (symbol, exchange, lastSale)
            |values ("AMD", "NASDAQ", 67.55),
            |       ("AAPL", "NYSE", 123.55),
            |       ("GE", "NASDAQ", 89.55),
            |       ("PEREZ", "OTCBB", 0.001),
            |       ("AMZN", "NYSE", 1234.55),
            |       ("INTC", "NYSE", 56.55)
            |""".stripMargin)
      assert(cost0 == IOCost(created = 2, destroyed = 1, inserted = 6, rowIDs = new RowIDRange(from = 0, to = 5)))
    }

    it("should accelerate queries via the index") {
      val (_, cost1, device1) = LollypopVM.searchSQL(Scope(),
        s"""|select * from $tableRef
            |where symbol is "AMZN"
            |""".stripMargin)
      assert(cost1 == IOCost(matched = 1, scanned = 1))
      assert(device1.toMapGraph == List(
        Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55)
      ))
    }

    it("should fail to insert a record with a duplicate key") {
      assertThrows[UniqueKeyAlreadyExistsError] {
        LollypopVM.executeSQL(Scope(),
          s"""|insert into $tableRef (symbol, exchange, lastSale)
              |values ("AMZN", "NYSE", 1187.33)
              |""".stripMargin)
      }
    }

    it("should fail to perform an individual field update results in a duplicate key violation") {
      assertThrows[UniqueKeyAlreadyExistsError] {
        Scope().getRowCollection(tableRef).updateField(rowID = 0, columnID = 0, newValue = Some("AMZN"))
      }
    }

    it("should perform an individual field update that does not result in a duplicate key violation") {
      Scope().getRowCollection(tableRef).updateField(rowID = 0, columnID = 0, newValue = Some("XYZ"))
    }

    it("should fail to perform an individual field metadata update results in a duplicate key violation") {
      assertThrows[UniqueKeyCannotBeDisabledError] {
        Scope().getRowCollection(tableRef).updateFieldMetadata(rowID = 0, columnID = 0, fmd = FieldMetadata(isActive = false))
      }
    }

    it("should fail to perform an individual field metadata update that does not result in a duplicate key violation") {
      Scope().getRowCollection(tableRef).updateFieldMetadata(rowID = 0, columnID = 2, fmd = FieldMetadata(isActive = false))
    }

    it("should fail to perform a row update that results in a duplicate key violation") {
      assertThrows[UniqueKeyAlreadyExistsError] {
        LollypopVM.executeSQL(Scope(),
          s"""|update $tableRef set symbol = 'AMZN'
              |where symbol is 'INTC'
              |""".stripMargin)
      }
    }

    it("should perform a row update that does not result in a duplicate key violation") {
      val (_, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|update $tableRef set symbol = 'XXX'
            |where symbol is 'INTC'
            |""".stripMargin)
      assert(cost == IOCost(updated = 1, scanned = 1, matched = 1))
    }

    it("should re-read the previously updated row") {
      val (_, cost, result) = LollypopVM.searchSQL(Scope(),
        s"""|select * from $tableRef where symbol is 'XXX'
            |""".stripMargin)
      assert(cost == IOCost(scanned = 1, matched = 1))
      assert(result.toMapGraph == List(Map("symbol" -> "XXX", "exchange" -> "NYSE", "lastSale" -> 56.55)))
    }

    it("should replace (delete then update) an existing record") {
      val (_, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|delete from $tableRef where symbol is 'GE' &&
            |update $tableRef set symbol = 'GE', exchange = 'AMEX', lastSale = 89.55 where symbol is 'PEREZ'
            |""".stripMargin)
      assert(cost == IOCost() || cost == IOCost(matched = 2, scanned = 2, updated = 1, deleted = 1))
    }

    it("should fail to perform a row metadata update results in a duplicate key violation") {
      assertThrows[UniqueKeyAlreadyExistsError] {
        Scope().getRowCollection(tableRef).updateRowMetadata(rowID = 2, rmd = RowMetadata())
      }
    }

    it("should fail to perform a row metadata update that does not result in a duplicate key violation") {
      Scope().getRowCollection(tableRef).updateFieldMetadata(rowID = 0, columnID = 2, fmd = FieldMetadata(isActive = false))
    }

    it("should re-read all previously updated rows") {
      val (_, cost, result) = LollypopVM.searchSQL(Scope(),
        s"""|select * from $tableRef
            |""".stripMargin)
      assert(cost == IOCost(scanned = 5, matched = 5))
      assert(result.toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "NASDAQ"),
        Map("symbol" -> "AAPL", "exchange" -> "NYSE", "lastSale" -> 123.55),
        Map("symbol" -> "GE", "exchange" -> "AMEX", "lastSale" -> 89.55),
        Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55),
        Map("symbol" -> "XXX", "exchange" -> "NYSE", "lastSale" -> 56.55)
      ))
    }

  }

}
