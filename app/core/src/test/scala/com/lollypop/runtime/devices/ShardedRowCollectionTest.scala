package com.lollypop.runtime.devices

import com.github.ldaniels528.lollypop.StockQuote._
import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.{LollypopCompiler, Scope}
import com.lollypop.util.ResourceHelper._
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.io.Source
import scala.util.Random

class ShardedRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  implicit val compiler: LollypopCompiler = LollypopCompiler()
  implicit val scope: Scope = Scope()

  private val newQuote: () => Map[String, Any] = {
    () => Map("symbol" -> randomSymbol, "exchange" -> randomExchange, "lastSale" -> randomPrice, "lastSaleTime" -> randomDate)
  }

  private val columns = Seq(
    Column(name = "symbol", `type` = ColumnType("String", size = 8)),
    Column(name = "exchange", `type` = ColumnType("String", size = 8)),
    Column(name = "lastSale", `type` = ColumnType("Double")),
    Column(name = "lastSaleTime", `type` = ColumnType("Long"))
  ).map(_.toTableColumn)

  describe(classOf[ShardedRowCollection].getSimpleName) {

    it("should shard data across devices") {
      val deviceA = ShardedRowCollection(createTempNS(), columns, shardSize = 1000, RowCollectionBuilder())
      val deviceB = FileRowCollection(columns)

      // write the contents of stocks.csv to both devices
      Source.fromFile("./app/examples/stocks.csv").use(_.getLines() foreach { line =>
        val array = line.split("[,]")
        val row = Map(columns.map(_.name) zip array: _*)
        deviceA.insert(row.toRow(deviceA))
        deviceB.insert(row.toRow(deviceB))
      })

      // row count should match
      logger.info(s"deviceA: ${deviceA.getLength} row(s)")
      logger.info(s"deviceB: ${deviceB.getLength} row(s)")
      assert(deviceA.getLength == deviceB.getLength)

      // prepare some random offsets
      val offsets: Seq[Long] = {
        val random = new Random()
        Seq(0L, deviceA.getLength / 2, deviceA.getLength - 1) ++ (for {_ <- 0 until 17} yield random.nextLong(deviceA.getLength))
      }

      // ensure deviceA is the same as deviceB for the random offsets
      offsets foreach { offset =>
        val rowA = deviceA.apply(offset)
        val rowB = deviceB.apply(offset)
        logger.info(s"rowA: ${rowA.toMap}")
        logger.info(s"rowB: ${rowB.toMap}")
        assert(rowA == rowB)
      }

      // shrink both devices
      deviceA.setLength(1000)
      deviceB.setLength(1000)
      assert(deviceA.getLength == deviceB.getLength)
    }

    it("should shard large datasets over several devices") {
      implicit val device: ShardedRowCollection =
        ShardedRowCollection(createTempNS(), columns, shardSize = 1000, RowCollectionBuilder())

      for (n <- 1 to 10000) {
        if (n % 1000 == 0) logger.info(s"Wrote $n rows...")
        device.insert(newQuote().toRow)
      }
    }

  }

}
