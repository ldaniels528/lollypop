package com.lollypop.runtime.devices

import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.datatypes.{DateTimeType, Float64Type, StringType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.{ROWID, Scope}
import com.lollypop.util.DateHelper
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import java.util.Date

/**
 * File Row Collection Test Suite
 */
class FileRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[FileRowCollection].getSimpleName) {
    implicit val rootScope: Scope = Scope()

    it("should perform CRUD operations") {
      val columns = Seq(
        Column(name = "symbol", `type` = ColumnType("String", size = 6)),
        Column(name = "exchange", `type` = ColumnType("String", size = 6)),
        Column(name = "lastSale", `type` = ColumnType("Double")),
        Column(name = "lastSaleTime", `type` = ColumnType("DateTime"))
      ).map(_.toTableColumn)

      FileRowCollection(columns) use { device =>
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

    it("should read/write row data") {
      val columns = Seq(
        TableColumn(name = "symbol", `type` = StringType(8)),
        TableColumn(name = "exchange", `type` = StringType(8)),
        TableColumn(name = "lastSale", `type` = Float64Type),
        TableColumn(name = "lastSaleTime", `type` = DateTimeType)
      )

      // get a reference to the file
      val file = createTempFile()

      // create a row-oriented file device
      FileRowCollection(columns, file) use { implicit device =>
        // truncate the file
        device.setLength(0)
        val rowID: ROWID = 0

        // write a record to the table
        val row0 = Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 99.98, "lastSaleTime" -> new Date()).toRow
        device.update(rowID, row0)

        // retrieve the record from the table
        val row1 = device(rowID)

        // show the fields
        row1.fields foreach { field =>
          logger.info(field.value.toString)
        }

      }
    }

  }

}
