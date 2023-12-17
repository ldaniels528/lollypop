package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.devices.{HashIndexRowCollection, MultiIndexRowCollection}
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import lollypop.io.{IOCost, RowIDRange}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class CreateIndexTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateIndex].getSimpleName) {

    it("should compile: create index stocks#symbol") {
      val results = compiler.compile(
        """|create index stocks#symbol
           |""".stripMargin)
      assert(results == CreateIndex(ref = DatabaseObjectRef("stocks#symbol"), ifNotExists = false))
    }

    it("should compile: create index if not exists com.acme.roadrunner#trap") {
      val results = compiler.compile(
        """|create index if not exists com.acme.roadrunner#trap
           |""".stripMargin)
      assert(results == CreateIndex(ref = DatabaseObjectRef("com.acme.roadrunner#trap"), ifNotExists = true))
    }

    it("should decompile: create index stocks#symbol") {
      verify(
        """|create index stocks#symbol
           |""".stripMargin)
    }

    it("should decompile: create index if not exists com.acme.roadrunner#trap") {
      verify(
        """|create index if not exists com.acme.roadrunner#trap
           |""".stripMargin)
    }

  }

  describe(classOf[HashIndexRowCollection].getSimpleName) {

    it("should execute create index statements") {
      val tableRef = DatabaseObjectRef(getTestTableName) // CreateIndexTest
      val indexRef = DatabaseObjectRef.InnerTable(tableRef, "exchange") // CreateIndexTest#exchange
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $tableRef &&
            |create table $tableRef (symbol: String(8), exchange: String(8), lastSale: Double) containing values
            |       ("AMD", "NASDAQ", 67.55),
            |       ("AAPL", "NYSE", 123.55),
            |       ("GE", "NASDAQ", 89.55),
            |       ("PEREZ", "OTCBB", 0.001),
            |       ("AMZN", "NYSE", 1234.55),
            |       ("INTC", "NYSE", 56.55) &&
            |
            |create index $indexRef
            |""".stripMargin)
      assert(cost0 == IOCost(created = 2, destroyed = 1, inserted = 6, shuffled = 6, rowIDs = new RowIDRange(from = 0, to = 5)))

      // create an instance of the index
      val hashIndex = HashIndexRowCollection(ns = indexRef.toNS(scope0))

      // perform a query against the index
      val (_, cost1, device1) = LollypopVM.searchSQL(
        scope0.withVariable(name = "stocks", value = hashIndex, isReadOnly = true),
        s"""|@stocks where exchange is "NYSE" and lastSale < 100
            |""".stripMargin)
      device1.tabulate().foreach(logger.info)
      assert(cost1 == IOCost(matched = 1, scanned = 3))
      assert(device1.toMapGraph == List(
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55)
      ))
      hashIndex.close()
    }

  }

  describe(classOf[MultiIndexRowCollection].getSimpleName) {

    it("should execute create index statements with multiple indices") {
      val tableRef = DatabaseObjectRef(getTestTableName + "2") // CreateIndexTest2
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $tableRef &&
            |create table $tableRef (
            |   symbol: String(8),
            |   exchange: String(8),
            |   lastSale: Double
            |) &&
            |insert into $tableRef (symbol, exchange, lastSale)
            |values ("AMD", "NASDAQ", 67.55),
            |       ("AAPL", "NYSE", 123.55),
            |       ("GE", "NASDAQ", 89.55),
            |       ("PEREZ", "OTCBB", 0.001),
            |       ("AMZN", "NYSE", 1234.55),
            |       ("INTC", "NYSE", 56.55)  &&
            |create index $tableRef#symbol  &&
            |create index $tableRef#exchange
            |""".stripMargin)
      assert(cost0 == IOCost(destroyed = 1, created = 3, inserted = 6, shuffled = 12, rowIDs = new RowIDRange(from = 0, to = 5)))

      // create an instance of the index
      val multiIndex = MultiIndexRowCollection(ns = tableRef.toNS(scope0))

      // perform a query against the exchange index
      val (_, _, device1) = LollypopVM.searchSQL(
        scope0.withVariable(name = "stocks", value = multiIndex, isReadOnly = true),
        s"""|@stocks where exchange is "NYSE" and lastSale < 100
            |""".stripMargin)
      assert(device1.toMapGraph == List(
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "lastSale" -> 56.55)
      ))

      // perform a query against the symbol index
      val (_, _, device2) = LollypopVM.searchSQL(
        scope0.withVariable(name = "stocks", value = multiIndex, isReadOnly = true),
        s"""|@stocks where symbol is "AMZN"
            |""".stripMargin)
      assert(device2.toMapGraph == List(
        Map("symbol" -> "AMZN", "exchange" -> "NYSE", "lastSale" -> 1234.55)
      ))
      multiIndex.close()
    }

  }

}
