package com.qwery.runtime.devices

import com.github.ldaniels528.qwery.StockQuote.{randomQuote, randomURID}
import com.github.ldaniels528.qwery.{GenericData, StockQuote}
import com.qwery.implicits.{MagicBoolImplicits, MagicImplicits}
import com.qwery.runtime.devices.BasicIterator.RichBasicIterator
import com.qwery.runtime.devices.ProductCollectionTest.StockTicker
import com.qwery.runtime.devices.RowCollectionZoo.ProductToRowCollection
import com.qwery.runtime.{ColumnInfo, DatabaseManagementSystem, DatabaseObjectRef, QweryVM, Scope, time}
import com.qwery.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import qwery.io.{IOCost, RowIDRange}

import java.util.Date
import scala.annotation.meta.field
import scala.reflect.ClassTag

/**
 * Product Collection Test Suite
 */
class ProductCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val bonjourStock = randomQuote.copy(symbol = "BELLE")
  private val stocks10 = List(
    StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L),
    StockQuote(symbol = "KFFQ", exchange = "NYSE", lastSale = 22.92, lastSaleTime = 1597181591000L),
    StockQuote(symbol = "GTKK", exchange = "NASDAQ", lastSale = 240.14, lastSaleTime = 1596835991000L),
    StockQuote(symbol = "KNOW", exchange = "OTCBB", lastSale = 357.21, lastSaleTime = 1597872791000L),
    StockQuote(symbol = "ABC", exchange = "NYSE", lastSale = 34.67, lastSaleTime = 1666219704275L),
    StockQuote(symbol = "GREY", exchange = "NYSE", lastSale = 122.03, lastSaleTime = 1666219704275L),
    StockQuote(symbol = "ZEBRA", exchange = "OTCBB", lastSale = 0.07891, lastSaleTime = 1666219704275L),
    StockQuote(symbol = "GO", exchange = "NYSE", lastSale = 5.67, lastSaleTime = 1666219704275L),
    StockQuote(symbol = "BEE", exchange = "OTHEROTC", lastSale = 0.0987, lastSaleTime = 1666214444275L),
    StockQuote(symbol = "FDS", exchange = "NYSE", lastSale = 212.98, lastSaleTime = 1666219704275L),
  )

  describe(classOf[ProductCollection[_]].getSimpleName) {

    it("should extract values from a product class") {
      val ps = ProductCollection[GenericData]()
      val data = GenericData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592204400000L, _id = 1087L)
      val values = ps.toKeyValues(data)
      assert(values == List(
        "_id" -> Some(1087L),
        "idValue" -> Some("Hello"),
        "idType" -> Some("World"),
        "responseTime" -> Some(307),
        "reportDate" -> Some(1592204400000L)
      ))
    }

    it("should populate a product class with values") {
      import com.qwery.util.OptionHelper.implicits.risky._
      val ps = ProductCollection[GenericData]()
      val data = ps.convertRow(Row(id = 1087, metadata = RowMetadata(), columns = ps.host.columns, fields = List(
        Field(name = "_id", metadata = FieldMetadata(), value = None),
        Field(name = "idValue", metadata = FieldMetadata(), value = "Hello"),
        Field(name = "idType", metadata = FieldMetadata(), value = "World"),
        Field(name = "responseTime", metadata = FieldMetadata(), value = 307),
        Field(name = "reportDate", metadata = FieldMetadata(), value = 1592215200000L)
      )))
      assert(data == GenericData(idValue = "Hello", idType = "World", responseTime = 307, reportDate = 1592215200000L, _id = 1087))
    }

    it("should append a collection of items to the collection") {
      val coll = ProductCollection[StockQuote]()
      val stat = eval("coll.append(Seq(..))", coll.insert(stocks10))
      assert(stat.inserted == stocks10.length)
    }

    it("should append an item to the collection") {
      val coll = ProductCollection[StockQuote]()
      val stat = eval(s"coll.append($bonjourStock)", coll.insert(bonjourStock))
      assert(stat == IOCost(inserted = 1, rowIDs = RowIDRange(0L)))
    }

    it("should retrieve one record with row metadata by its offset (rowID)") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val rowID = randomURID(coll.host)
      eval(f"coll.apply($rowID)", coll(rowID))
    }

    it("should compute avg(lastSale)") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      eval("coll.avg(_.lastSale)", coll.avg(_.lastSale))
    }

    it("should count all items") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val total = eval("coll.countMetadata(_.isActive)", coll.countWhereMetadata(_.isActive))
      assert(total == stocks10.length)
    }

    it("should count the items where: lastSale <= 100") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      eval("coll.count(_.lastSale <= 100))", coll.count(_.lastSale <= 100))
    }

    it("should test existence where: lastSale >= 100") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val lastSale_lt_500 = eval("coll.exists(_.lastSale >= 950)", coll.exists(_.lastSale >= 100))
      assert(lastSale_lt_500)
    }

    it("should filter for items where: lastSale < 100") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val items = eval("coll.filter(_.lastSale < 100)", coll.filter(_.lastSale < 100))
      assert(items.nonEmpty)
    }

    it("should filter for items where not: lastSale < 100") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val items = eval("coll.filterNot(_.lastSale < 100)", coll.filterNot(_.lastSale < 100))
      assert(items.nonEmpty)
    }

    it("should find one item where: lastSale > 100") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val items = eval("coll.find(_.lastSale > 100)", coll.find(_.lastSale > 100))
      assert(items.nonEmpty)
    }

    it("should indicate whether all items satisfy: lastSale < 1000") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val result = eval("coll.forall(_.lastSale < 1000))", coll.forall(_.lastSale < 1000))
      assert(result)
    }

    it("should retrieve one item by its offset (rowID)") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val rowID = randomURID(coll.host)
      val item_? = eval(f"coll.get($rowID)", coll.get(rowID))
      assert(item_?.nonEmpty)
    }

    it("should retrieve the first item (sequentially) from the collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val item_? = eval("coll.headOption", coll.headOption)
      assert(item_?.contains(stocks10.head))
    }

    it("should find index where: symbol == 'XXX'") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      coll.insert(bonjourStock.copy(symbol = "XXX"))
      val index = eval("""coll.indexWhere(_.symbol == "XXX")""", coll.indexWhere(_.symbol == "XXX"))
      assert(index.exists(_ != -1))
    }

    it("should find index of a specific record") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      coll.insert(bonjourStock)
      logger.info(s"coll.lastOption => ${coll.lastOption}")
      val index = eval(s"""coll.indexOf($bonjourStock)""", coll.indexOf(bonjourStock))
      assert(index != -1)
    }

    it("should produce an iterator") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val quote_? = coll.iterator ~> { it => it.hasNext ==> it.next() }
      assert(quote_? contains StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L))
    }

    it("should retrieve the last item (sequentially) from the collection") {
      val coll = newCollection[StockQuote]
      coll insert stocks10
      val item_? = eval("coll.lastOption", coll.lastOption)
      assert(item_?.contains(stocks10.last))
    }

    it("should retrieve the file length") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val length = eval("coll.length", coll.getLength)
      assert(length >= coll.countWhereMetadata(_.isActive))
    }

    it("should compute max(lastSale)") {
      val coll = newCollection[StockQuote]
      coll insert stocks10
      val value = eval("coll.max(_.lastSale)", coll.max(_.lastSale))
      assert(value == 357.21)
    }

    it("should compute min(lastSale)") {
      val coll = ProductCollection[StockQuote]()
      coll insert stocks10
      val value = eval("coll.min(_.lastSale)", coll.min(_.lastSale))
      assert(value == 0.07891)
    }

    it("should compute the 95th percentile for lastSale") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val value = eval("coll.percentile(0.95)(_.lastSale)", coll.percentile(0.95)(_.lastSale))
      assert(value > 0)
    }

    it("should remove records where: lastSale > 100") {
      val coll = newCollection[StockQuote]
      coll insert stocks10

      val cost = eval("coll.delete(_.lastSale > 100)", coll.delete(_.lastSale > 100))
      cost.toRowCollection.tabulate().foreach(logger.info)
      assert(cost.deleted > 0)
    }

    it("should show that length and count() can be inconsistent") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      info(s"coll.countMetadata(_.isActive) ~> ${coll.countWhereMetadata(_.isActive)} but coll.length ~> ${coll.getLength}")
    }

    it("should retrieve a limited number of items from the collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      assert(coll.take(3).toList == stocks10.slice(0, 3))
    }

    it("should reverse the collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      assert(coll.reverse.toList == stocks10.reverse)
    }

    it("should extract a slice of items from the collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val result = coll.slice(rowID0 = 0, rowID1 = 2)
      assert(result.toList == List(
        StockQuote(symbol = "BXXG", exchange = "NASDAQ", lastSale = 147.63, lastSaleTime = 1596317591000L),
        StockQuote(symbol = "KFFQ", exchange = "NYSE", lastSale = 22.92, lastSaleTime = 1597181591000L),
        StockQuote(symbol = "GTKK", exchange = "NASDAQ", lastSale = 240.14, lastSaleTime = 1596835991000L)
      ))
    }

    it("should sort the collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      val items = eval("coll.sortBy(_.symbol)", coll.sortBy(_.symbol))
      items.take(5).foreach(item => println(item.toString))
    }

    it("should sort the collection (in place)") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      eval("coll.sortInPlace(_.symbol)", coll.sortInPlace(_.symbol))
      coll.toList.take(5).foreach(item => println(item.toString))
    }

    it("should compute sum(lastSale)") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      eval("coll.sum(_.lastSale)", coll.sum(_.lastSale))
    }

    it("should tail a collection") {
      val coll = ProductCollection[StockQuote]()
      coll.insert(stocks10)
      eval("coll.tail", coll.tail)
    }

  }

  describe(classOf[ShardedRowCollection].getSimpleName) {
    val quotes2 = (0 to 1).map(_ => randomQuote)
    val quotes10 = (0 to 10).map(_ => randomQuote)

    it("should read/write data into a single partition") {
      val coll = ProductCollection[StockQuote](RowCollection.builder
        .withProduct[StockQuote]
        .withSharding(shardSize = 3))
      coll insert quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index + 1}] $q") }
      assert(coll.getLength == 2)
    }

    it("should read/write data into a single partition on the edge of a second partition") {
      val coll = ProductCollection[StockQuote](RowCollection.builder.withProduct[StockQuote].withSharding(2))
      coll insert quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index + 1}] $q") }
      assert(coll.getLength == 2)
    }

    it("should read/write data into two partitions") {
      val coll = ProductCollection[StockQuote](RowCollection.builder.withProduct[StockQuote].withSharding(1))
      coll insert quotes2
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index + 1}] $q") }
      assert(coll.getLength == 2)
    }

    it("should read/write data into multiple partitions") {
      val coll = ProductCollection[StockQuote](RowCollection.builder.withProduct[StockQuote].withSharding(2))
      coll insert quotes10
      coll.toList.zipWithIndex.foreach { case (q, index) => logger.info(s"[${index + 1}] $q") }
      assert(coll.getLength == 11)
    }

    it(s"should read an existing table as a ${classOf[ProductCollection[_]].getSimpleName}") {
      val ref = DatabaseObjectRef(getClass.getSimpleName)
      val (scope, cost, _) = QweryVM.executeSQL(Scope(),
        s"""|drop if exists $ref &&
            |create table $ref (
            |   symbol: String(5),
            |   exchange: String(6),
            |   lastSale: Double,
            |   lastSaleTime: DateTime
            |) containing (
            |    |--------------------------------------------------------|
            |    | symbol | exchange | lastSale | lastSaleTime            |
            |    |--------------------------------------------------------|
            |    | NKWI   | OTCBB  |   98.9501 | 2022-09-04T23:36:47.846Z |
            |    | AQKU   | NASDAQ |   68.2945 | 2022-09-04T23:36:47.860Z |
            |    | WRGB   | AMEX   |   46.8355 | 2022-09-04T23:36:47.862Z |
            |    | ESCN   | NYSE   |   42.5934 | 2022-09-04T23:36:47.865Z |
            |    | NFRK   | AMEX   |   28.2808 | 2022-09-04T23:36:47.864Z |
            |    |--------------------------------------------------------|
            |)
            |""".stripMargin)
      assert(cost == IOCost(created = 1, destroyed = 1, inserted = 5, rowIDs = RowIDRange(0, 1, 2, 3, 4)))
      val device = DatabaseManagementSystem.readProductCollection[StockTicker](ref.toNS(scope))
      assert(device.toList == List(
        StockTicker("NKWI", "OTCBB", 98.9501, DateHelper("2022-09-04T23:36:47.846Z")),
        StockTicker("AQKU", "NASDAQ", 68.2945, DateHelper("2022-09-04T23:36:47.860Z")),
        StockTicker("WRGB", "AMEX", 46.8355, DateHelper("2022-09-04T23:36:47.862Z")),
        StockTicker("ESCN", "NYSE", 42.5934, DateHelper("2022-09-04T23:36:47.865Z")),
        StockTicker("NFRK", "AMEX", 28.2808, DateHelper("2022-09-04T23:36:47.864Z"))
      ))
    }

  }

  private def eval[A](label: String, f: => A): A = {
    val (results, runTime) = time(f)
    val output = results match {
      case items: ProductCollection[_] => f"(${items.getLength} items)"
      case value: Double => f"${value.toDouble}%.2f"
      case items: Seq[_] => f"(${items.length} items)"
      case it: Iterator[_] => if (it.hasNext) s"<${it.next()}, ...>" else "<empty>"
      case x => x.toString
    }
    logger.info(f"$label ~> $output [$runTime%.2f msec]")
    results
  }

  private def newCollection[A <: Product : ClassTag]: ProductCollection[A] = {
    ProductCollection[A]()
  }

  private def newCollectionWithData: ProductCollection[StockQuote] = {
    val coll = ProductCollection[StockQuote]()
    coll.insert(stocks10)
    coll
  }

}

object ProductCollectionTest {

  case class StockTicker(@(ColumnInfo@field)(maxSize = 6) symbol: String,
                         @(ColumnInfo@field)(maxSize = 6) exchange: String,
                         lastSale: Double,
                         lastSaleTime: Date)
}