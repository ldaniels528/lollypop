package com.lollypop.runtime.devices

import com.github.ldaniels528.lollypop.StockQuote.randomQuote
import com.lollypop.implicits.MagicImplicits
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{DateTimeType, Float32Type, StringType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionPerformanceTest.{TestInput, TestOutput}
import com.lollypop.util.ResourceHelper._
import com.lollypop.util.Tabulator
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.Properties

/**
 * Row Collection Performance Test
 * @example {{{
 * |---------------------------------------------------------------------------------------------------------------|
 * | device name                         | read rows/msec | write rows/msec | read time (msec) | write time (msec) |
 * |---------------------------------------------------------------------------------------------------------------|
 * | ByteArrayRowCollection              |       267.0225 |           3.095 |             74.9 |         6461.9302 |
 * | FileRowCollection                   |       199.6125 |           4.075 |         100.1941 |         4907.8747 |
 * | ModelRowCollection                  |      3132.4964 |          4.0417 |           6.3846 |         4948.4079 |
 * | ShardedRowCollection (ByteArray) x4 |       638.7824 |        263.0447 |          31.3095 |           76.0326 |
 * | ShardedRowCollection (Model) x4     |      4383.6413 |       2573.1825 |           4.5624 |            7.7724 |
 * | ShardedRowCollection (Model) x8     |      1864.8058 |        628.3795 |          10.7249 |           31.8279 |
 * | ShardedRowCollection (RAF) x4       |       228.4495 |         83.4958 |          87.5466 |          239.5327 |
 * |---------------------------------------------------------------------------------------------------------------|
 * }}}
 */
class RowCollectionPerformanceTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private val columns = Seq(
    TableColumn(name = "symbol", `type` = StringType(5)),
    TableColumn(name = "exchange", `type` = StringType(6)),
    TableColumn(name = "lastSale", `type` = Float32Type),
    TableColumn(name = "tradeDate", `type` = DateTimeType))

  private val expectedCount = Properties.envOrElse("input_size", "2000").toInt
  private val rows = generateStocks(expectedCount)
  private val testInputs = List(
    TestInput(label = "ByteArrayRowCollection", builder = _.withColumns(columns).withMemorySupport(expectedCount)),
    TestInput(label = "ModelRowCollection", builder = _.withColumns(columns).withMemorySupport()),
    TestInput(label = "FileRowCollection", builder = _.withColumns(columns)),
    TestInput(label = "ShardedRowCollection (RAF) x4", builder = _.withColumns(columns).withSharding(shardSize = expectedCount / 4)),
    TestInput(label = "ShardedRowCollection (ByteArray) x4", builder = _.withColumns(columns).withSharding(shardSize = expectedCount / 4, builder = _.withMemorySupport(expectedCount / 4))),
    TestInput(label = "ShardedRowCollection (Model) x4", builder = _.withColumns(columns).withSharding(shardSize = expectedCount / 4, builder = _.withMemorySupport()))
  )

  describe(classOf[RowCollection].getSimpleName) {

    it("should test all device types") {
      val mappings = testInputs
        .map(doTestCycle(_, rows))
        .sortBy(_.label)
        .map { case TestOutput(label, writeTime, writeRate, readTime, readRate) =>
          List(label, scale(readRate), scale(writeRate), scale(readTime), scale(writeTime)).map(Option.apply)
        }
      Tabulator.tabulate(List("device name", "read rows/msec", "write rows/msec", "read time (msec)", "write time (msec)"), mappings)
        .foreach(logger.info)
    }

  }

  private def generateStocks(total: Int): List[Row] = {
    val rs = RecordStructure(columns)
    for {
      _ <- (0 until total).toList
      q = randomQuote
      row = Map("symbol" -> q.symbol, "exchange" -> q.exchange, "lastSale" -> q.lastSale, "tradeDate" -> q.lastSaleTime).toRow(0L)(rs)
    } yield row
  }

  private def doTestCycle(input: TestInput, rows: List[Row]): TestOutput = {
    input.builder(RowCollectionBuilder()).build use { coll =>
      coll.setLength(0)
      val writeTime = doWriteTest(coll, rows)
      val writeRate = expectedCount / writeTime
      val readTime = doReadTest(coll)
      val readRate = expectedCount / readTime
      TestOutput(input.label, writeTime, writeRate, readTime, readRate)
    }
  }

  private def doReadTest(coll: RowCollection): Double = {
    implicit val scope: Scope = Scope()
    val startTime = System.nanoTime()
    coll.iterateWhere()(_.isActive) { (_, _) => }
    val elapsedTime = (System.nanoTime() - startTime) / 1e+6
    elapsedTime
  }

  private def doWriteTest(coll: RowCollection, rows: List[Row]): Double = {
    val startTime = System.nanoTime()
    coll.insert(rows)
    val elapsedTime = (System.nanoTime() - startTime) / 1e+6
    elapsedTime
  }

  private def scale(value: Double): Double = {
    val places = 4
    Math.pow(10, places) ~> { s => (value * s).toLong / s }
  }

}

object RowCollectionPerformanceTest {

  case class TestInput(label: String, builder: RowCollectionBuilder => RowCollectionBuilder)

  case class TestOutput(label: String, writeTime: Double, writeRate: Double, readTime: Double, readRate: Double)

}