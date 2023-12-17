package com.lollypop.runtime.devices

import com.lollypop.language._
import com.lollypop.runtime._
import com.lollypop.runtime.devices.errors.FormatNotSpecified
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.aggregation.Count
import com.lollypop.util.DateHelper
import lollypop.io.IOCost
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * External Files Table Row Collection Test
 */
class ExternalFilesTableRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val refIS = DatabaseObjectRef("temp.devices.ExternalFilesTableRowCollectionTest_IndexedSearch")
  private val refCL = DatabaseObjectRef("temp.devices.ExternalFilesTableRowCollectionTest_CompanyList")
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[ExternalFilesTableRowCollection].getSimpleName) {

    it("should support indexed searches") {
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $refIS &&
            |create external table $refIS (
            |   symbol: String(6),
            |   exchange: String(6),
            |   lastSale: Double,
            |   transactionTime: DateTime
            |) containing { location: './app/examples/stocks.csv', headers: false }
            |""".stripMargin)
      assert(cost0 == IOCost(destroyed = 1, created = 1))

      // retrieve a record
      val ((_, cost3, device3), ts3) = time(LollypopVM.searchSQL(Scope(),
        s"""|select * from $refIS where symbol is 'JURY'
            |""".stripMargin))
      logger.info(f"non-indexed query took $ts3%.1f msec")
      assert(device3.toMapGraph == List(
        Map("symbol" -> "JURY", "exchange" -> "NASDAQ", "lastSale" -> 40.27, "transactionTime" -> DateHelper("2020-08-15T21:33:11.000Z"))
      ))
      assert(cost3 == IOCost(matched = 1, scanned = 10001))

      // create an index for faster reading
      val (_, cost4, _) = LollypopVM.executeSQL(scope0,
        s"""|create index $refIS#symbol
            |""".stripMargin)
      assert(cost4 == IOCost(created = 1, shuffled = 10001))

      // retrieve a record using the index
      val ((_, cost5, device5), ts5) = time(LollypopVM.searchSQL(Scope(),
        s"""|// close the table so that it gets reloaded w/the index
            |ns('$refIS').close()
            |select * from $refIS where symbol is 'JURY'
            |""".stripMargin))
      logger.info(f"indexed query took $ts5%.1f msec")
      assert(device5.toMapGraph == List(
        Map("symbol" -> "JURY", "exchange" -> "NASDAQ", "lastSale" -> 40.27, "transactionTime" -> DateHelper("2020-08-15T21:33:11.000Z"))
      ))
      assert(cost5 == IOCost(matched = 1, scanned = 1))
    }

    it("should create an external table") {
      val (_, cost, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $refCL
            |create external table $refCL (
            |   Symbol: String(10),
            |   Name: String(70),
            |   LastSale: Double,
            |   MarketCap: Double,
            |   IPOyear: String(8),
            |   Sector: String(70),
            |   Industry: String(70),
            |   SummaryQuote: String(70),
            |   Reserved: String(20)
            |) containing { location: './app/examples/companylist/csv/', headers: true, null_values: ['n/a'] }
            |""".stripMargin)
      assert(cost.created == 1)
    }

    it("should fetch a specific row") {
      implicit val scope: Scope = Scope()
      val rowID: ROWID = 19
      ExternalFilesTableRowCollection(refCL) use { device =>
        val row_? = device.getRowScope(rowID).getCurrentRow
        info(s"row($rowID) => ${row_?.map(_.toMap)}")
        assert(row_?.map(_.toMap).toList == List(Map(
          "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "LastSale" -> 3.05, "MarketCap" -> 1.743e+7
        )))
      }
    }

    it("should fetch a contiguous slice of rows") {
      implicit val scope: Scope = Scope()
      ExternalFilesTableRowCollection(refCL) use { device =>
        val rows = device.slice(rowID0 = 1L, rowID1 = 5L)
        rows.tabulate().foreach(logger.info)
        assert(rows.toMapGraph == List(
          Map("Name" -> "Aberdeen Asia-Pacific Income Fund Inc", "SummaryQuote" -> "https://www.nasdaq.com/symbol/fax", "Symbol" -> "FAX", "IPOyear" -> "1986", "LastSale" -> 4.195, "MarketCap" -> 1.06E9),
          Map("Name" -> "Aberdeen Australia Equity Fund Inc", "SummaryQuote" -> "https://www.nasdaq.com/symbol/iaf", "Symbol" -> "IAF", "LastSale" -> 6.1, "MarketCap" -> 1.3873E8),
          Map("Name" -> "Aberdeen Emerging Markets Equity Income Fund, Inc.", "SummaryQuote" -> "https://www.nasdaq.com/symbol/aef", "Symbol" -> "AEF", "LastSale" -> 6.8, "MarketCap" -> 4.1139E8),
          Map("Name" -> "Aberdeen Global Income Fund, Inc.", "SummaryQuote" -> "https://www.nasdaq.com/symbol/fco", "Symbol" -> "FCO", "IPOyear" -> "1992", "LastSale" -> 8.03, "MarketCap" -> 7.006E7),
          Map("SummaryQuote" -> "https://www.nasdaq.com/symbol/acu", "Industry" -> "Industrial Machinery/Components", "Symbol" -> "ACU", "IPOyear" -> "1988", "LastSale" -> 21.46, "MarketCap" -> 7.241E7, "Sector" -> "Capital Goods", "Name" -> "Acme United Corporation.")
        ))
      }
    }

    it("should queries rows using select()") {
      implicit val scope: Scope = Scope()
      ExternalFilesTableRowCollection(refCL) use { device =>
        val (_, _, rs) = device
          .select("Sector".f, Count("*".f) as "total")
          .groupBy("Sector".f)
          .orderBy("Sector".asc)
          .execute()

        rs.toMapGraph.zipWithIndex foreach { case (row, n) => logger.info(f"[$n%02d] $row") }
        assert(rs.toMapGraph == List(
          Map("Sector" -> "Basic Industries", "total" -> 317),
          Map("Sector" -> "Capital Goods", "total" -> 372),
          Map("Sector" -> "Consumer Durables", "total" -> 148),
          Map("Sector" -> "Consumer Non-Durables", "total" -> 238),
          Map("Sector" -> "Consumer Services", "total" -> 829),
          Map("Sector" -> "Energy", "total" -> 315),
          Map("Sector" -> "Finance", "total" -> 1054),
          Map("Sector" -> "Health Care", "total" -> 860),
          Map("Sector" -> "Miscellaneous", "total" -> 157),
          Map("Sector" -> "Public Utilities", "total" -> 273),
          Map("Sector" -> "Technology", "total" -> 625),
          Map("Sector" -> "Transportation", "total" -> 124),
          Map("total" -> 1563)
        ))
      }
    }

    it("should query rows using SQL") {
      val (_, _, rs) = LollypopVM.searchSQL(Scope(),
        s"""|select Sector, total: count(*)
            |from $refCL
            |group by Sector
            |order by Sector asc
            |""".stripMargin
      )
      rs.toMapGraph.zipWithIndex foreach { case (row, n) => logger.info(f"[$n%02d] $row") }
      assert(rs.toMapGraph == List(
        Map("Sector" -> "Basic Industries", "total" -> 317),
        Map("Sector" -> "Capital Goods", "total" -> 372),
        Map("Sector" -> "Consumer Durables", "total" -> 148),
        Map("Sector" -> "Consumer Non-Durables", "total" -> 238),
        Map("Sector" -> "Consumer Services", "total" -> 829),
        Map("Sector" -> "Energy", "total" -> 315),
        Map("Sector" -> "Finance", "total" -> 1054),
        Map("Sector" -> "Health Care", "total" -> 860),
        Map("Sector" -> "Miscellaneous", "total" -> 157),
        Map("Sector" -> "Public Utilities", "total" -> 273),
        Map("Sector" -> "Technology", "total" -> 625),
        Map("Sector" -> "Transportation", "total" -> 124),
        Map("total" -> 1563)
      ))
    }

    it("should not allow modifications") {
      implicit val scope: Scope = Scope()
      ExternalFilesTableRowCollection(refCL) use { implicit device =>
        val outcome = Try(device.insert(Map(
          "Sector" -> "Health Care", "Name" -> "American Shared Hospital Services",
          "SummaryQuote" -> "https://www.nasdaq.com/symbol/ams", "Industry" -> "Medical Specialities",
          "Symbol" -> "AMS", "IPOyear" -> "n/a", "LastSale" -> 3.05, "MarketCap" -> 1.743e+7
        ).toRow))
        info(s"outcome: $outcome")
        assert(outcome.isFailure && outcome.failed.get.getMessage == "Table is read-only")
      }
    }

    it("should determine the length of the device") {
      implicit val scope: Scope = Scope()
      ExternalFilesTableRowCollection(refCL) use { device =>
        info(s"the length of the device is ${device.getLength} record(s)")
        assert(device.getLength == 6875L)
      }
    }

    it("should determine the sizeInBytes of the device") {
      implicit val scope: Scope = Scope()
      ExternalFilesTableRowCollection(refCL) use { device =>
        info(s"the sizeInBytes of the device is ${device.getLength} record(s)")
        assert(device.sizeInBytes == 2557500L)
      }
    }

    it("should fail if the file format could not be determined") {
      val ref = DatabaseObjectRef("temp.devices.FailFast")
      assertThrows[FormatNotSpecified] {
        LollypopVM.executeSQL(Scope(),
          s"""|drop if exists $ref &&
              |create external table $ref (
              |   symbol: String(5),
              |   exchange: String(6),
              |   lastSale: Double,
              |   transactionTime: Long
              |) containing { location: 'examples/src/test/resources/log4j.properties', headers: false }
              |select * from $ref
              |""".stripMargin)
      }

    }

  }

}
