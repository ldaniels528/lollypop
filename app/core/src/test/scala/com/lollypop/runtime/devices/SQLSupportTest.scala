package com.lollypop.runtime.devices

import com.lollypop.language._
import com.lollypop.language.implicits._
import com.lollypop.language.models.{Column, ColumnType}
import com.lollypop.runtime._
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.devices.TableColumn.implicits.SQLToColumnConversion
import com.lollypop.runtime.instructions.expressions.aggregation.{Count, Max}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * SQL Support Tests
 */
class SQLSupportTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[SQLSupport].getSimpleName) {

    it("should perform a selection of the data") {
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
      val (_, _, resultsA) = device.select("*".f).where("exchange".f === "OTCBB").execute()
      resultsA.tabulate().foreach(logger.info)
      assert(resultsA.toMapGraph == List(
        Map("exchange" -> "OTCBB", "symbol" -> "SCAM.OB", "lastSale" -> 0.001)
      ))

      // perform an aggregate query
      val (_, _, resultsB) = device
        .select("exchange".f, Max("lastSale".f) as "maxLastSale", Count("*".f) as "total")
        .groupBy("exchange".f)
        .orderBy("total".desc)
        .execute()

      resultsB.tabulate().foreach(logger.info)
      assert(resultsB.toMapGraph == List(
        Map("exchange" -> "NASDAQ", "maxLastSale" -> 399.22, "total" -> 3),
        Map("exchange" -> "OTCBB", "maxLastSale" -> 0.001, "total" -> 1)
      ))
    }

    it("should generate the appropriate SQL for GROUP BY, HAVING, ORDER BY and LIMIT") {
      val (_, _, resultA) =
        """||----------------------------------------------------------|
           || exchange  | symbol | lastSale | lastSaleTime             |
           ||----------------------------------------------------------|
           || NASDAQ    | KS     | 146.6212 | 2023-11-16T13:52:46.158Z |
           || OTHER_OTC | LQIHA  |   0.6002 | 2023-11-16T13:53:10.275Z |
           || OTHER_OTC | LOVR   |   0.2860 | 2023-11-16T13:53:07.677Z |
           || OTHER_OTC | YQLZ   |   0.4082 | 2023-11-16T13:52:58.871Z |
           || AMEX      | KUXYW  | 155.3511 | 2023-11-16T13:53:06.296Z |
           ||----------------------------------------------------------|
           |""".stripMargin.searchSQL(Scope())

      val model = resultA
        .select("exchange".f, Max("lastSale".f) as "maxLastSale", Count("*".f) as "total")
        .groupBy("exchange".f)
        .having("total".f > 1)
        .orderBy("total".desc)
        .limit(1)
        .toModel

      // verify the SQL
      val sql = model.copy(from = Some(DatabaseObjectRef("Stocks"))).toSQL
      assert(sql == """select exchange, maxLastSale: max(lastSale), total: count(*) from Stocks group by exchange having total > 1 order by total desc limit 1""")
    }

  }

}
