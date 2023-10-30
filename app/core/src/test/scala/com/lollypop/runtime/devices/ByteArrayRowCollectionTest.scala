package com.lollypop.runtime.devices

import com.github.ldaniels528.lollypop.StockQuote.randomURID
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Inequality._
import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.datatypes.TableType
import com.lollypop.runtime.devices.BasicIterator.RichBasicIterator
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.{LollypopCompiler, ROWID, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ByteArrayRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()
  implicit val scope: Scope = Scope()

  private val columns = Seq(
    Column(name = "symbol", `type` = ColumnType("String", size = 4)),
    Column(name = "exchange", `type` = ColumnType("String", size = 6)),
    Column(name = "lastSale", `type` = ColumnType("Double")),
    Column(name = "lastSaleTime", `type` = ColumnType("Long"))
  ).map(_.toTableColumn)
  private val stocks = List(
    Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
    Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
    Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
    Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
  )

  describe(classOf[ByteArrayRowCollection].getSimpleName) {

    it("should perform CRUD operations") {
      val tableType = TableType(
        capacity = 1024,
        columns = Seq(
          Column(name = "symbol", `type` = ColumnType("String", size = 6)),
          Column(name = "exchange", `type` = ColumnType("String", size = 6)),
          Column(name = "lastSale", `type` = ColumnType("Double")),
          Column(name = "lastSaleTime", `type` = ColumnType("DateTime"))
        ).map(_.toTableColumn))

      ByteArrayRowCollection(tableType) use { device =>
        val stocks = List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
          Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L)))
        stocks.foreach { stock =>
          device.insert(stock.toRow(rowID = 0L)(device))
        }
        device.tabulate().foreach(logger.info)
        assert(device.toMapGraph == List(
          Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
          Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
          Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
          Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
        ))
      }
    }

    it("should count the rows where: `exchange` is 'NASDAQ')") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.push(row.toRow(device)))
      val stat = device.countWhere(condition = Some("exchange".f === "NASDAQ".v))
      assert(stat.matched == 2)
    }

    it("should delete the rows where: `exchange` is 'NASDAQ')") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.push(row.toRow(device)))
      device.tabulate() foreach logger.info
      val stat = device.deleteWhere(condition = Some("exchange".f === "NASDAQ".v), limit = None)
      assert(stat.matched == 2)
      assert(device.getRowSummary == RowSummary(active = 2, deleted = 2))
      val results = device
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should filter out duplicates") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length * 2)
      (stocks ::: stocks).foreach(row => device.push(row.toRow(device)))
      device.tabulate() foreach logger.info
      val results = device.distinct
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should reverse the rows of the device") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.push(row.toRow(device)))
      val results = device.reverse
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L)
      ))
    }

    it("should count the rows where: isActive is true") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.push(row.toRow(device)))
      val count = eval("device.countMetadata(_.isActive)", device.countWhereMetadata(_.isActive))
      assert(count == stocks.length)
    }

    it("should read an individual field value (via column index)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val field = eval(f"device.getField(rowID = 0, columnID = 0)", device.readField(rowID = 0, columnID = 0))
      assert(field.value.contains("BXXG"))
    }

    it("should read an individual field value (via column name)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val field = eval(f"device.getField(rowID = 0, column = 'lastSale')", device.readField(rowID = 0, columnID = 2))
      assert(field.value.contains(147.63))
    }

    it("should retrieve one row by its offset (rowID)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val rowID = randomURID(device)
      val row = eval(f"device.readRow($rowID)", device.apply(rowID))

      logger.info(s"rowID: \t ${row.id}")
      logger.info(s"metadata: \t ${row.metadata}")
      row.fields.zipWithIndex foreach { case (field, index) =>
        logger.info(f"[$index%02d]: \t ${field.name} - ${field.metadata}")
      }
    }

    it("should retrieve one row metadata by its offset (rowID)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val rowID = randomURID(device)
      val rmd = eval(f"device.readRowMetadata($rowID)", device.readRowMetadata(rowID))
      logger.info(s"rmd => $rmd")
    }

    it("should retrieve the record size") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val recordSize = eval("device.recordSize", device.recordSize)
      assert(recordSize == 39)
    }

    it("should remove records by its offset (rowID)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val rowID = randomURID(device)
      val before = device.countWhereMetadata(_.isActive)
      eval(f"device.deleteRow($rowID)", device.delete(rowID))
      val after = device.countWhereMetadata(_.isActive)
      assert(before - after == 1)
    }

    it("should reverse the collection (in place)") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      eval("device.reverseInPlace()", device.reverseInPlace())
      assert(device.toMapGraph == List(
        Map("exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
      ))
    }

    it("should retrieve the row statistics") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      device.delete(0)
      val stats = eval("device.getRowSummary", device.getRowSummary)
      assert(stats == RowSummary(active = 3, deleted = 1))
    }

    it("should swap the position of two rows") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val offset1: ROWID = randomURID(device)
      val offset2: ROWID = randomURID(device)
      Seq(offset1, offset2).map(device.apply).foreach(item => logger.info(s"BEFORE: $item"))
      eval(s"device.swapRows($offset1, $offset2)", device.swap(offset1, offset2))
      Seq(offset1, offset2).map(device.apply).foreach(item => logger.info(s"after: $item"))
    }

    it("should shrink the collection by 25%") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      val newSize = (device.getLength * 0.75).toInt
      eval(f"device.shrinkTo($newSize)", device.setLength(newSize))
      assert(device.getLength <= newSize)
    }

    it("should remove dead entries from of the collection") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      device.tabulate() foreach logger.info
      eval("Seq(0L, 3L).map(device.deleteRow)", Seq(0L, 3L).map(device.delete))
      eval("device.compact()", device.compact())
      device.tabulate() foreach logger.info
      assert(device.getLength == stocks.length - 2)
    }

    it("should read a single field") {
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))
      println(s"coll contains ${device.getLength} items")
      device.toMapGraph.foreach(row => logger.info(s"row: $row"))
      println()

      // read the symbol from the 1st record
      assert(grabField(rowID = 0, columnIndex = 0).value.contains("BXXG"))

      // read the exchange from the 2nd record
      assert(grabField(rowID = 1, column = "exchange").value.contains("NYSE"))

      // read the lastSale from the 3rd record
      assert(grabField(rowID = 2, column = "lastSale").value.contains(240.14))

      // read the lastSaleTime from the 4th record
      assert(grabField(rowID = 3, columnIndex = 3).value.contains(1597872791000L))
    }

    it("should set default column values upon encode") {
      val columns = Seq(
        Column("symbol", ColumnType("String", 4)),
        Column("exchange", ColumnType("String", 6)),
        Column("lastSale", ColumnType("Double")),
        Column("lastSaleTime", ColumnType("Long")),
        Column("description", ColumnType("String", 255)).copy(defaultValue = Some("N/A".v))).map(_.toTableColumn)
      implicit val device: RowCollection = ByteArrayRowCollection(columns, capacity = stocks.length)
      stocks.foreach(row => device.insert(row.toRow(device)))

      device.toMapGraph.foreach(row => logger.info(s"device: $row"))
      assert(device.toMapGraph == List(
        Map("description" -> "N/A", "exchange" -> "NASDAQ", "symbol" -> "BXXG", "lastSale" -> 147.63, "lastSaleTime" -> 1596317591000L),
        Map("description" -> "N/A", "exchange" -> "NYSE", "symbol" -> "KFFQ", "lastSale" -> 22.92, "lastSaleTime" -> 1597181591000L),
        Map("description" -> "N/A", "exchange" -> "NASDAQ", "symbol" -> "GTKK", "lastSale" -> 240.14, "lastSaleTime" -> 1596835991000L),
        Map("description" -> "N/A", "exchange" -> "OTCBB", "symbol" -> "KNOW", "lastSale" -> 357.21, "lastSaleTime" -> 1597872791000L)
      ))
    }

    it("should allow iteration of a query result") {
      implicit val scope: Scope = Scope()
      val columns = Seq(
        Column(name = "symbol", `type` = ColumnType("String", size = 4)),
        Column(name = "exchange", `type` = ColumnType("String", size = 6)),
        Column(name = "lastSale", `type` = ColumnType("Double")),
        Column(name = "lastSaleTime", `type` = ColumnType("DateTime"))
      ).map(_.toTableColumn)

      // write rows to the device
      val device = FileRowCollection(columns)
      val stocks = List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      )
      stocks.zipWithIndex.foreach { case (stock, rowID) =>
        device.insert(stock.toRow(rowID)(device))
      }

      assert(device.iterator.toMapGraph == List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L))
      ))
    }

  }

  private def eval[A](label: String, f: => A): A = {
    val (results, runTime) = time(f)
    val output = results match {
      case items: RowCollection => f"(${items.getLength} items)"
      case value: Double => f"${value.toDouble}%.2f"
      case items: Seq[_] => f"(${items.length} items)"
      case it: Iterator[_] => if (it.hasNext) s"<${it.next()}, ...>" else "<empty>"
      case x => x.toString
    }
    logger.info(f"$label ~> $output [$runTime%.2f msec]")
    results
  }

  private def grabField(rowID: ROWID, columnIndex: Int)(implicit device: RowCollection): Field = {
    val field@Field(name, fmd, value_?) = device.readField(rowID, columnIndex)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

  private def grabField(rowID: ROWID, column: String)(implicit device: RowCollection): Field = {
    val columnID = device.columns.indexWhere(_.name == column)
    assert(columnID >= 0, s"Column '$column' not found")
    val field@Field(name, fmd, value_?) = device.readField(rowID, columnID)
    println(s"$name: ${value_?} -> $fmd")
    field
  }

}
