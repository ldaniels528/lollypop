package com.lollypop.runtime.devices

import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import lollypop.io.IOCost
import lollypop.lang.BitArray

class HashIndexRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val baseDataSet = List(
    Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
    Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
    Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
    Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
    Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
    Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
    Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
    Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
  )

  describe(classOf[HashIndexRowCollection].getSimpleName) {
    implicit val scope0: Scope = Scope()

    it("should support basic read/write operations") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (_, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // verify the data
      logger.info("BASE table")
      hashIndex.baseTable.tabulate().foreach(logger.info)
      assert(hashIndex.baseTable.toMapGraph == baseDataSet)
      logger.info("")
      logger.info("HASH table")
      hashIndex.hashTable.tabulate().foreach(logger.info)
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray(2)),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5, 6))
      ))
      // lookup rows where exchange is "OTCBB"
      assert(hashIndex.searchIndex(searchValue = Some("OTCBB")).toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011)
      ))
    }

    it("should support update operations on a non-indexed column") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (scope2, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // update a row
      val (_, cost3, _) = LollypopVM.executeSQL(scope2.withVariable(name = "stocks", value = Some(hashIndex), isReadOnly = true),
        s"""|update @@stocks set symbol = 'XXX' where symbol is 'ZZY'
            |""".stripMargin)
      assert(cost3.updated == 1)

      // verify the data
      assert(hashIndex.baseTable.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "XXX", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray(2)),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5, 6))
      ))
      assert(hashIndex.searchIndex(searchValue = Some("NYSE")).toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88)
      ))
    }

    it("should support update operations on an indexed column") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (scope2, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // update a row
      val (_, cost3, _) = LollypopVM.executeSQL(scope2.withVariable(name = "stocks", value = Some(hashIndex), isReadOnly = true),
        s"""|update @@stocks set exchange = 'NASDAQ' where symbol is 'ZZY'
            |""".stripMargin)
      assert(cost3.updated == 1)

      // verify the data
      assert(hashIndex.baseTable.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "NASDAQ", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(6, 7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray(2)),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5))
      ))
      assert(hashIndex.searchIndex(searchValue = Some("OTHER_OTC")).isEmpty)
    }

    it("should support update field metadata update operations on a non-indexed column") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (_, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // update a field
      hashIndex.updateFieldMetadata(rowID = 0, columnID = 2, FieldMetadata(isActive = false))

      // verify the results
      assert(hashIndex.baseTable.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE"),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray(2)),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5, 6))
      ))
      assert(hashIndex.searchIndex(searchValue = Some("AMEX")).toMapGraph == List(
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95)
      ))
    }

    it("should support field metadata update operations to deactivate an indexed column") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (_, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // update the indexed field
      hashIndex.updateFieldMetadata(rowID = 0, columnID = 1, FieldMetadata(isActive = false))

      // verify the results
      assert(hashIndex.baseTable.toMapGraph == List(
        Map("symbol" -> "AAXX", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
      assert(hashIndex.hashTable.toMapGraph.map { m => m.getOrElse("exchange", "") -> m("index") } == List(
        ("", BitArray(0)), ("NASDAQ", BitArray(7)), ("NYSE", BitArray(1, 4)), ("AMEX", BitArray(2)), ("OTCBB", BitArray(3, 5, 6))
      ))
      assert(hashIndex.searchIndex(searchValue = None).toMapGraph == List(
        Map("symbol" -> "AAXX", "lastSale" -> 56.12)
      ))
    }

    it("should support row metadata update operations") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data
      val (_, hashIndex) = insertDataWithHashIndex(ref)(scope1)

      // deactivate the metadata of a row
      hashIndex.updateRowMetadata(rowID = 2, RowMetadata(isAllocated = false))

      // verify the results
      assert(hashIndex.baseTable.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "UPEX", "exchange" -> "NYSE", "lastSale" -> 116.24),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE", "lastSale" -> 77.88),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011),
        Map("symbol" -> "GABY", "exchange" -> "NASDAQ", "lastSale" -> 13.99)
      ))
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray()),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5, 6))
      ))
      assert(hashIndex.searchIndex(searchValue = Some("AMEX")).isEmpty)
    }

    it("should support rebuilding the hash index") {
      // create the base table
      val ref = DatabaseObjectRef(getTestTableName)
      val (scope1, _) = createStocksTable()

      // insert some data into the base table
      val (_, baseTable) = insertDataWithoutHashIndex(ref)(scope1)
      assert(baseTable.toMapGraph == baseDataSet)

      // create the hash index
      val hashIndex = HashIndexRowCollection(ns = ref.toNS.copy(columnName = Some("exchange")))

      // verify that no results are returned prior to rebuilding the index
      assert(hashIndex.hashTable.isEmpty)
      assert(hashIndex.searchIndex(searchValue = Some("OTCBB")).isEmpty)

      // rebuild the index
      val cost3 = hashIndex.rebuild()
      assert(cost3.shuffled == 8)

      // verify the data within the index
      assert(hashIndex.hashTable.toMapGraph.toSet == Set(
        Map("exchange" -> "NASDAQ", "index" -> BitArray(7)),
        Map("exchange" -> "NYSE", "index" -> BitArray(0, 1, 4)),
        Map("exchange" -> "AMEX", "index" -> BitArray(2)),
        Map("exchange" -> "OTCBB", "index" -> BitArray(3, 5, 6))
      ))
      assert(hashIndex.searchIndex(searchValue = Some("OTCBB")).toMapGraph == List(
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887),
        Map("symbol" -> "ZZZ", "exchange" -> "OTCBB", "lastSale" -> 0.0001),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB", "lastSale" -> 0.0011)
      ))
    }

  }

  private def createStocksTable()(implicit scope0: Scope): (Scope, IOCost) = {
    val ref = DatabaseObjectRef(getTestTableName)
    val (scope1, cost1, _) = LollypopVM.executeSQL(scope0,
      s"""|drop if exists $ref
          |create table $ref (symbol: String(8), exchange: String(8), lastSale: Double)
          |""".stripMargin)
    assert(cost1.created == 1)
    (scope1, cost1)
  }

  private def insertData[A <: RowCollection](rc: A)(implicit scope1: Scope): (Scope, A) = {
    // insert some data
    val (scope2, cost2, _) = LollypopVM.executeSQL(scope1.withVariable(name = "stocks", value = Some(rc), isReadOnly = true),
      s"""|insert into @@stocks (symbol, exchange, lastSale)
          |from
          |    |------------------------------|
          |    | symbol | exchange | lastSale |
          |    |------------------------------|
          |    | AAXX   | NYSE     |    56.12 |
          |    | UPEX   | NYSE     |   116.24 |
          |    | XYZ    | AMEX     |    31.95 |
          |    | ABC    | OTCBB    |    5.887 |
          |    | TRIX   | NYSE     |    77.88 |
          |    | ZZZ    | OTCBB    |   0.0001 |
          |    | ZZY    | OTCBB    |   0.0011 |
          |    | GABY   | NASDAQ   |    13.99 |
          |    |------------------------------|
          |""".stripMargin)
    assert(cost2.inserted == 8)
    (scope2, rc)
  }

  private def insertDataWithHashIndex(ref: DatabaseObjectRef)(implicit scope1: Scope): (Scope, HashIndexRowCollection) = {
    // get the hash-index
    val hashIndex = HashIndexRowCollection(ns = ref.toNS.copy(columnName = Some("exchange")))

    // insert some data
    insertData(hashIndex)
  }

  private def insertDataWithoutHashIndex(ref: DatabaseObjectRef)(implicit scope1: Scope): (Scope, RowCollection) = {
    // get the base table
    val baseTable = scope1.getRowCollection(ref)

    // insert some data
    insertData(baseTable)
  }

}
