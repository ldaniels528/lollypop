package com.qwery.runtime.devices

import com.qwery.language.models.Expression.implicits._
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.language.models.{Column, ColumnType}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo.createTempNS
import com.qwery.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.qwery.runtime.instructions.expressions.aggregation.{Count, Max}
import com.qwery.runtime.{QweryCompiler, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * SQL Support Tests
 */
class SQLSupportTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[SQLSupport].getSimpleName) {

    implicit val scope: Scope = Scope()
    val columns = List(
      Column("symbol", ColumnType("String", 8)),
      Column("exchange", ColumnType("String", 8)),
      Column("lastSale", ColumnType("Double"))
    ).map(_.toTableColumn)

    implicit val device: ByteArrayRowCollection = {
      val rs = RecordStructure(columns)
      val array = new Array[Byte](5 * rs.recordSize)
      new ByteArrayRowCollection(createTempNS(), columns, array) with SQLSupport
    }

    it("should perform a selection of the data") {
      // insert some rows
      val rows = List(
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "lastSale" -> 99.98),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 399.22),
        Map("symbol" -> "GE", "exchange" -> "NASDAQ", "lastSale" -> 66.11),
        Map("symbol" -> "SCAM.OB", "exchange" -> "OTCBB", "lastSale" -> 0.001)
      )
      rows.foreach(row => device.insert(row.toRow))
      assert(device.getLength == rows.length)

      // perform a simple query
      val (_, _, resultsA) = device.select("*".f).where("exchange".f === "OTCBB").search()
      resultsA.toMapGraph foreach (row => logger.info(s"row: $row"))
      assert(resultsA.toMapGraph == List(
        Map("exchange" -> "OTCBB", "symbol" -> "SCAM.OB", "lastSale" -> 0.001)
      ))

      // perform an aggregate query
      val (_, _, resultsB) = device
        .select("exchange".f, Max("lastSale".f) as "maxLastSale", Count("*".f) as "total")
        .groupBy("exchange".f)
        .orderBy("total".desc)
        .search()

      resultsB.toMapGraph foreach (row => logger.info(s"row: $row"))
      assert(resultsB.toMapGraph == List(
        Map("exchange" -> "NASDAQ", "maxLastSale" -> 399.22, "total" -> 3),
        Map("exchange" -> "OTCBB", "maxLastSale" -> 0.001, "total" -> 1)
      ))
    }

  }

}
