package com.lollypop.runtime.devices


import com.lollypop.language.dieIllegalType
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Inequality._
import com.lollypop.runtime.datatypes.{DateTimeType, Float64Type, StringType}
import com.lollypop.runtime.devices.RowCollectionZoo.createTempNS
import com.lollypop.runtime.{DatabaseObjectRef, LollypopVM, Scope}
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec
import lollypop.io.{IOCost, RowIDRange}

class PartitionedRowCollectionTest extends AnyFunSpec {

  describe(classOf[PartitionedRowCollection[_]].getSimpleName) {

    it("should perform CRUD operations on in-memory partitioned tables") {
      val device = PartitionedRowCollection(createTempNS(), columns = Seq(
        TableColumn(name = "symbol", `type` = StringType),
        TableColumn(name = "exchange", `type` = StringType),
        TableColumn(name = "lastSale", `type` = Float64Type),
        TableColumn(name = "lastSaleTime", `type` = DateTimeType)),
        partitionColumnIndex = 1)
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope().withVariable("stocks", device),
        """|insert into @stocks (symbol, exchange, lastSale, lastSaleTime)
           |from (
           |    |---------------------------------------------------------|
           |    | symbol | exchange | lastSale | lastSaleTime             |
           |    |---------------------------------------------------------|
           |    | ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           |    | ABC    | OTCBB    |   8.1112 | 2022-09-04T09:36:51.007Z |
           |    | BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           |    | TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           |    | AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           |    | BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           |    | NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           |    | TRX    | NYSE     |  88.22   | 2022-09-04T09:12:53.009Z |
           |    | TRX    | NYSE     |  88.56   | 2022-09-04T09:12:57.706Z |
           |    | NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           |    | WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           |    | ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           |    | NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           |    | AAPL   | NASDAQ   | 100.01   | 2022-09-04T09:36:46.033Z |
           |    | AAPL   | NASDAQ   | 100.12   | 2022-09-04T09:36:48.459Z |
           |    | WRKR   | AMEX     | 100.12   | 2022-09-04T09:36:48.459Z |
           |    | BOOTY  | OTCBB    |  13.12   | 2022-09-04T09:51:13.111Z |
           |    |---------------------------------------------------------|
           |)
           |""".stripMargin)
      // verify the cost
      assert(cost0 == IOCost(inserted = 34, rowIDs = new RowIDRange(from = 0, to = 16)))

      // verify the table length
      assert(device.getLength == 17)

      // verify the partitions
      assert(device.getPartitionMap.map { case (key, rc) => key -> rc.toMapGraph } == Map(
        "NYSE" -> List(
          Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
          Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z"))
        ),
        "NASDAQ" -> List(
          Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z")),
          Map("symbol" -> "BKBK", "exchange" -> "NASDAQ", "lastSale" -> 78.1238, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.080Z")),
          Map("symbol" -> "NGA", "exchange" -> "NASDAQ", "lastSale" -> 23.6812, "lastSaleTime" -> DateHelper("2022-09-04T09:36:41.808Z")),
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.01, "lastSaleTime" -> DateHelper("2022-09-04T09:36:46.033Z")),
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
        ),
        "OTCBB" -> List(
          Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
          Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
          Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
          Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "NGINX", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 13.12, "lastSaleTime" -> DateHelper("2022-09-04T09:51:13.111Z"))
        ),
        "AMEX" -> List(
          Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 46.8355, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.111Z")),
          Map("symbol" -> "ESCN", "exchange" -> "AMEX", "lastSale" -> 42.5934, "lastSaleTime" -> DateHelper("2022-09-04T09:36:42.321Z")),
          Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
        )
      ))

      // verify the table
      assert(device.toMapGraph.toSet == Set(
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
        Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z")),
        Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z")),
        Map("symbol" -> "BKBK", "exchange" -> "NASDAQ", "lastSale" -> 78.1238, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.080Z")),
        Map("symbol" -> "NGA", "exchange" -> "NASDAQ", "lastSale" -> 23.6812, "lastSaleTime" -> DateHelper("2022-09-04T09:36:41.808Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.01, "lastSaleTime" -> DateHelper("2022-09-04T09:36:46.033Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
        Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "NGINX", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 13.12, "lastSaleTime" -> DateHelper("2022-09-04T09:51:13.111Z")),
        Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 46.8355, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.111Z")),
        Map("symbol" -> "ESCN", "exchange" -> "AMEX", "lastSale" -> 42.5934, "lastSaleTime" -> DateHelper("2022-09-04T09:36:42.321Z")),
        Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
      ))

      // perform a direct search
      val (_, cost1, result1) = device.search(Some("exchange".f === "NYSE".v))(scope0)
      assert(cost1 == IOCost(scanned = 3, matched = 3))
      assert(result1.toMapGraph == List(
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
        Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z"))
      ))

      // close the device
      device.close()
    }

    it("should perform CRUD operations on durable partitioned tables") {
      val (scope0, cost0, _) = LollypopVM.executeSQL(Scope(),
        """|namespace "temp.partitions"
           |drop if exists Stocks
           |create table Stocks (
           |   symbol: String(8),
           |   exchange: String(8),
           |   lastSale: Double,
           |   lastSaleTime: DateTime
           |) partitioned by ['exchange']
           |insert into Stocks (symbol, exchange, lastSale, lastSaleTime)
           |from (
           |    |---------------------------------------------------------|
           |    | symbol | exchange | lastSale | lastSaleTime             |
           |    |---------------------------------------------------------|
           |    | ABC    | OTCBB    |   8.0985 | 2022-09-04T09:36:47.846Z |
           |    | ABC    | OTCBB    |   8.1112 | 2022-09-04T09:36:51.007Z |
           |    | BOOTY  | OTCBB    |  17.5776 | 2022-09-04T09:37:11.332Z |
           |    | TREE   | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           |    | AQKU   | NASDAQ   |  68.2945 | 2022-09-04T09:36:51.112Z |
           |    | BKBK   | NASDAQ   |  78.1238 | 2022-09-04T09:36:47.080Z |
           |    | NGA    | NASDAQ   |  23.6812 | 2022-09-04T09:36:41.808Z |
           |    | TRX    | NYSE     |  88.22   | 2022-09-04T09:12:53.009Z |
           |    | TRX    | NYSE     |  88.56   | 2022-09-04T09:12:57.706Z |
           |    | NGINX  | OTCBB    |  0.00123 | 2022-09-04T09:12:53.009Z |
           |    | WRKR   | AMEX     |  46.8355 | 2022-09-04T09:36:48.111Z |
           |    | ESCN   | AMEX     |  42.5934 | 2022-09-04T09:36:42.321Z |
           |    | NFRK   | NYSE     |  28.2808 | 2022-09-04T09:36:47.675Z |
           |    | AAPL   | NASDAQ   | 100.01   | 2022-09-04T09:36:46.033Z |
           |    | AAPL   | NASDAQ   | 100.12   | 2022-09-04T09:36:48.459Z |
           |    | WRKR   | AMEX     | 100.12   | 2022-09-04T09:36:48.459Z |
           |    | BOOTY  | OTCBB    |  13.12   | 2022-09-04T09:51:13.111Z |
           |    |---------------------------------------------------------|
           |)
           |""".stripMargin)

      // verify the cost
      assert(cost0 == IOCost(created = 1, destroyed = 1, inserted = 34, rowIDs = new RowIDRange(from = 0, to = 16)))

      // get the PartitionedRowCollection
      val device = scope0.getRowCollection(DatabaseObjectRef("Stocks")) match {
        case p: PartitionedRowCollection[Any] => p
        case HostedRowCollection(p: PartitionedRowCollection[Any]) => p
        case x => dieIllegalType(x)
      }

      // verify the table length
      assert(device.getLength == 17)

      // verify the partitions
      assert(device.getPartitionMap.map { case (key, rc) => key -> rc.toMapGraph } == Map(
        "NYSE" -> List(
          Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
          Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z"))
        ),
        "NASDAQ" -> List(
          Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z")),
          Map("symbol" -> "BKBK", "exchange" -> "NASDAQ", "lastSale" -> 78.1238, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.080Z")),
          Map("symbol" -> "NGA", "exchange" -> "NASDAQ", "lastSale" -> 23.6812, "lastSaleTime" -> DateHelper("2022-09-04T09:36:41.808Z")),
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.01, "lastSaleTime" -> DateHelper("2022-09-04T09:36:46.033Z")),
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
        ),
        "OTCBB" -> List(
          Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
          Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
          Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
          Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "NGINX", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
          Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 13.12, "lastSaleTime" -> DateHelper("2022-09-04T09:51:13.111Z"))
        ),
        "AMEX" -> List(
          Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 46.8355, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.111Z")),
          Map("symbol" -> "ESCN", "exchange" -> "AMEX", "lastSale" -> 42.5934, "lastSaleTime" -> DateHelper("2022-09-04T09:36:42.321Z")),
          Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
        )
      ))

      // verify the table
      assert(device.toMapGraph.toSet == Set(
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
        Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z")),
        Map("symbol" -> "AQKU", "exchange" -> "NASDAQ", "lastSale" -> 68.2945, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.112Z")),
        Map("symbol" -> "BKBK", "exchange" -> "NASDAQ", "lastSale" -> 78.1238, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.080Z")),
        Map("symbol" -> "NGA", "exchange" -> "NASDAQ", "lastSale" -> 23.6812, "lastSaleTime" -> DateHelper("2022-09-04T09:36:41.808Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.01, "lastSaleTime" -> DateHelper("2022-09-04T09:36:46.033Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.0985, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.846Z")),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 8.1112, "lastSaleTime" -> DateHelper("2022-09-04T09:36:51.007Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 17.5776, "lastSaleTime" -> DateHelper("2022-09-04T09:37:11.332Z")),
        Map("symbol" -> "TREE", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "NGINX", "exchange" -> "OTCBB", "lastSale" -> 0.00123, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "BOOTY", "exchange" -> "OTCBB", "lastSale" -> 13.12, "lastSaleTime" -> DateHelper("2022-09-04T09:51:13.111Z")),
        Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 46.8355, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.111Z")),
        Map("symbol" -> "ESCN", "exchange" -> "AMEX", "lastSale" -> 42.5934, "lastSaleTime" -> DateHelper("2022-09-04T09:36:42.321Z")),
        Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
      ))

      // query a single partition
      val (scope1, cost1, result1) = LollypopVM.searchSQL(scope0.reset(),
        """|select * from Stocks
           |where exchange is 'NYSE'
           |""".stripMargin)
      assert(cost1 == IOCost(scanned = 3, matched = 3))
      assert(result1.toMapGraph == List(
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.22, "lastSaleTime" -> DateHelper("2022-09-04T09:12:53.009Z")),
        Map("symbol" -> "TRX", "exchange" -> "NYSE", "lastSale" -> 88.56, "lastSaleTime" -> DateHelper("2022-09-04T09:12:57.706Z")),
        Map("symbol" -> "NFRK", "exchange" -> "NYSE", "lastSale" -> 28.2808, "lastSaleTime" -> DateHelper("2022-09-04T09:36:47.675Z"))
      ))

      // query across partitions
      val (_, cost2, result2) = LollypopVM.searchSQL(scope1.reset(),
        """|select * from Stocks
           |where lastSale >= 100.0
           |""".stripMargin)
      assert(cost2 == IOCost(scanned = 17, matched = 3))
      assert(result2.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.01, "lastSaleTime" -> DateHelper("2022-09-04T09:36:46.033Z")),
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z")),
        Map("symbol" -> "WRKR", "exchange" -> "AMEX", "lastSale" -> 100.12, "lastSaleTime" -> DateHelper("2022-09-04T09:36:48.459Z"))
      ))

      // close the device
      device.close()
    }

  }

}
