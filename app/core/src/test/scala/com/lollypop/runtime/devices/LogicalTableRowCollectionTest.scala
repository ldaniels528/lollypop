package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{CLOB, TableType}
import com.lollypop.util.DateHelper
import lollypop.lang.Pointer
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class LogicalTableRowCollectionTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)

  describe(classOf[LogicalTableRowCollection].getSimpleName) {
    val clusteredRef = DatabaseObjectRef("stocksClustered")

    it("should create a table containing only clustered data types") {
      val ref = DatabaseObjectRef("passengersClustered")
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(createPassengerData(ref, textType = "String(64)")))
      assert(cost.inserted == 6)

      LogicalTableRowCollection(ref)(scope) use { implicit device =>
        assert(device.recordSize == 161)
        assert(device.sizeInBytes == 966)
        assert(device.clustered.recordSize == 161)
        assert(device.clustered.sizeInBytes == 966)
        assert(device.blob.sizeInBytes == 0)
        assert(device.toMapGraph == List(
          Map("lastName" -> "JONES", "airportCode" -> "SNA", "firstName" -> "GARRY", "age" -> 31, "id" -> 0),
          Map("lastName" -> "JONES", "airportCode" -> "SNA", "firstName" -> "TAMERA", "age" -> 28, "id" -> 1),
          Map("lastName" -> "JONES", "airportCode" -> "SNA", "firstName" -> "ERIC", "age" -> 5, "id" -> 2),
          Map("lastName" -> "ADAMS", "airportCode" -> "LAX", "firstName" -> "KAREN", "age" -> 58, "id" -> 3),
          Map("lastName" -> "ADAMS", "airportCode" -> "LAX", "firstName" -> "MIKE", "age" -> 55, "id" -> 4),
          Map("lastName" -> "DAVIS", "airportCode" -> "DTW", "firstName" -> "CHRIS", "age" -> 39, "id" -> 5)
        ))
      }
    }

    it("should create a table containing clustered and BLOB data types") {
      val ref = DatabaseObjectRef("passengersBlob")
      val (scope, outcome, _) = LollypopVM.executeSQL(Scope(), show(createPassengerData(ref, textType = "CLOB")))
      assert(outcome.inserted == 6)

      LogicalTableRowCollection(ref)(scope) use { implicit device =>
        assert(device.recordSize == 73)
        assert(device.sizeInBytes == 545)
        assert(device.clustered.recordSize == 73)
        assert(device.clustered.sizeInBytes == 438)
        assert(device.blob.sizeInBytes == 107)
        assert(device.toMapGraph == List(
          Map("lastName" -> CLOB("JONES"), "airportCode" -> "SNA", "firstName" -> CLOB("GARRY"), "age" -> 31, "id" -> 0),
          Map("lastName" -> CLOB("JONES"), "airportCode" -> "SNA", "firstName" -> CLOB("TAMERA"), "age" -> 28, "id" -> 1),
          Map("lastName" -> CLOB("JONES"), "airportCode" -> "SNA", "firstName" -> CLOB("ERIC"), "age" -> 5, "id" -> 2),
          Map("lastName" -> CLOB("ADAMS"), "airportCode" -> "LAX", "firstName" -> CLOB("KAREN"), "age" -> 58, "id" -> 3),
          Map("lastName" -> CLOB("ADAMS"), "airportCode" -> "LAX", "firstName" -> CLOB("MIKE"), "age" -> 55, "id" -> 4),
          Map("lastName" -> CLOB("DAVIS"), "airportCode" -> "DTW", "firstName" -> CLOB("CHRIS"), "age" -> 39, "id" -> 5)
        ))
        assert(device.clustered.toMapGraph == List(
          Map("airportCode" -> "SNA", "age" -> 31, "lastName" -> Pointer(0, 9, 9), "firstName" -> Pointer(9, 9, 9), "id" -> 0),
          Map("airportCode" -> "SNA", "age" -> 28, "lastName" -> Pointer(18, 9, 9), "firstName" -> Pointer(27, 10, 10), "id" -> 1),
          Map("airportCode" -> "SNA", "age" -> 5, "lastName" -> Pointer(37, 9, 9), "firstName" -> Pointer(46, 8, 8), "id" -> 2),
          Map("airportCode" -> "LAX", "age" -> 58, "lastName" -> Pointer(54, 9, 9), "firstName" -> Pointer(63, 9, 9), "id" -> 3),
          Map("airportCode" -> "LAX", "age" -> 55, "lastName" -> Pointer(72, 9, 9), "firstName" -> Pointer(81, 8, 8), "id" -> 4),
          Map("airportCode" -> "DTW", "age" -> 39, "lastName" -> Pointer(89, 9, 9), "firstName" -> Pointer(98, 9, 9), "id" -> 5)
        ))
      }
    }

    it("should create a table containing a clustered inner-table") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(), show(createStockDataSQL(clusteredRef, capacity = 10)))
      LogicalTableRowCollection(clusteredRef)(scope) use { implicit device =>
        assert(device.recordSize == 226)
        assert(device.sizeInBytes == 1130)
        assert(device.clustered.recordSize == 226)
        assert(device.clustered.sizeInBytes == 1130)
        assert(device.blob.sizeInBytes == 0)
        assert(device.tenant.getResource(columnName = "transactions").isEmpty)
        assert(device.toMapGraph == List(
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
            Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
            Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
            Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
          ))
        ))
      }
    }

    it("should retrieve rows from a clustered inner-table") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|${show(createStockDataSQL(clusteredRef, capacity = 10))}
            |select * from $clusteredRef where symbol is 'SHMN'
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should update rows of a clustered inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(clusteredRef, capacity = 10)}
            |update $clusteredRef
            |set transactions = [
            |   {"price":35.11, "transactionTime":"2021-08-05T19:23:12.000Z"},
            |   {"price":35.83, "transactionTime":"2021-08-05T19:23:15.000Z"},
            |   {"price":36.03, "transactionTime":"2021-08-05T19:23:17.000Z"}
            |]
            |where symbol is 'AMD'
            |""".stripMargin))
      logger.info(s"cost: $cost")
      assert(cost.updated == 1)
      assert(scope.getRowCollection(clusteredRef).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should insert embedded rows into a clustered inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(clusteredRef, capacity = 10)}
            |insert into $clusteredRef#transactions (price, transactionTime)
            |values (35.11, "2021-08-05T19:23:12.000Z"),
            |       (35.83, "2021-08-05T19:23:15.000Z"),
            |       (36.03, "2021-08-05T19:23:17.000Z")
            |where symbol is 'AMD'
            |""".stripMargin))
      assert(cost.inserted == 8)
      assert(scope.getRowCollection(clusteredRef).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should modify embedded rows from a clustered inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(clusteredRef, capacity = 10)}
            |update $clusteredRef#transactions
            |set price = 0.0012
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |limit 1
            |""".stripMargin))
      assert(cost.updated == 1)
      assert(scope.getRowCollection(clusteredRef).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0012, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should remove embedded rows from a clustered inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(clusteredRef, capacity = 10)}
            |delete from $clusteredRef#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin))
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(clusteredRef).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should write/read data to/from BLOB storage") {
      val (_, _, outcome) = LollypopVM.executeSQL(Scope(),
        s"""|namespace "temp.devices"
            |drop if exists Downloads
            |create table Downloads (
            |   id: RowNumber,
            |   name: String(10),
            |   content: CLOB
            |)
            |val downloads = ns("temp.devices.Downloads")
            |val ptr = downloads.writeBLOB("DANIELS")
            |let result: String = downloads.readBLOB(ptr)
            |result
            |""".stripMargin
      )
      assert(outcome == "DANIELS")
    }

  }

  describe(classOf[EmbeddedInnerTableRowCollection].getSimpleName) {
    val ref = DatabaseObjectRef("stocksBlob")

    it("should create a table containing a BLOB inner-table") {
      val (scope, _, device) = LollypopVM.searchSQL(Scope(), show(createStockDataSQL(ref, isPointer = true, capacity = 10)))
      device.tabulate() foreach logger.info
      LogicalTableRowCollection(ref)(scope) use { implicit device =>
        assert(device.recordSize == 226)
        assert(device.sizeInBytes == 1250)
        assert(device.clustered.recordSize == 52)
        assert(device.clustered.sizeInBytes == 260)
        assert(device.blob.sizeInBytes == 990)
        assert(device.tenant.getResource(columnName = "transactions").isEmpty)
        assert(device.clustered.toMapGraph == List(
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> Pointer(0, 198, 198)),
          Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> Pointer(198, 198, 198)),
          Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> Pointer(396, 198, 198)),
          Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> Pointer(594, 198, 198)),
          Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> Pointer(792, 198, 198))
        ))

        // verify the external dataset
        val blobStorage = device.blob
        val tableType_? = device.columns
          .collect { case c if c.name == "transactions" => c.`type` }
          .collect { case t: TableType => t }
        val results = for {
          tableType <- tableType_?
          row <- device.clustered.toMapGraph
          ptr <- row.get("transactions").toList.collect { case p: Pointer => p }
          rc = blobStorage.readCollection(tableType, ptr)
        } yield rc.toMapGraph

        assert(results == List(
          List(Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          List(Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          List(Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          List(Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          List(
            Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
            Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")))
        ))
      }
    }

    it("should retrieve a field the rows of a BLOB inner-table") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(), show(createStockDataSQL(ref, isPointer = true, capacity = 10)))
      val device = scope.getRowCollection(ref)
      assert(device.readField(0, 0).value contains "AAPL")
      assert(device.readField(4, 2).value.collect { case r: EmbeddedInnerTableRowCollection => r.toMapGraph }
        .contains(List(
          Map("price" -> 0.0010, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        )))
    }

    it("should retrieve rows from a BLOB inner-table") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, isPointer = true, capacity = 10)}
            |select * from $ref where symbol is 'SHMN'
            |""".stripMargin))
      assert(device.toMapGraph == List(
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should delete rows from a BLOB inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, isPointer = true, capacity = 10)}
            |delete from $ref where __id is 1
            |""".stripMargin))
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should update rows of a BLOB inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, isPointer = true, capacity = 10)}
            |update $ref
            |set transactions = [
            |   {"price":35.11, "transactionTime":"2021-08-05T19:23:12.000Z"},
            |   {"price":35.83, "transactionTime":"2021-08-05T19:23:15.000Z"},
            |   {"price":36.03, "transactionTime":"2021-08-05T19:23:17.000Z"},
            |]
            |where symbol is 'AMD'
            |""".stripMargin))
      assert(cost.updated == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should insert embedded rows into a BLOB inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, isPointer = true, capacity = 10)}
            |insert into $ref#transactions (price, transactionTime)
            |values (35.11, "2021-08-05T19:23:12.000Z"),
            |       (35.83, "2021-08-05T19:23:15.000Z"),
            |       (36.03, "2021-08-05T19:23:17.000Z")
            |where symbol is 'AMD'
            |""".stripMargin))
      assert(cost.inserted == 8)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should modify embedded rows from a BLOB inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, capacity = 10, isPointer = true)}
            |update $ref#transactions
            |set price = 0.0012
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |limit 1
            |""".stripMargin))
      assert(cost.updated == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0012, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should remove embedded rows from a BLOB inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref, capacity = 10, isPointer = true)}
            |delete from $ref#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin))
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

  }

  describe(classOf[MultiTenantRowCollection].getSimpleName) {
    val ref = DatabaseObjectRef("stocksMultiTenant")

    it("should create a table containing a multi-tenant inner-table") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(), show(createStockDataSQL(ref)))
      LogicalTableRowCollection(ref)(scope) use { implicit device =>
        assert(device.recordSize == 55)
        assert(device.sizeInBytes == 474)
        assert(device.clustered.recordSize == 52)
        assert(device.clustered.sizeInBytes == 260)
        assert(device.blob.sizeInBytes == 100)
        assert(device.tenant.getResource(columnName = "transactions").map(_.recordSize) contains 19)
        assert(device.tenant.getResource(columnName = "transactions").map(_.sizeInBytes) contains 114)
        assert(device.toMapGraph == List(
          Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
            Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
            Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
          )),
          Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
            Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
            Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
          ))
        ))
      }
    }

    it("should retrieve a field from a multi-tenant inner-table") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(), show(createStockDataSQL(ref)))
      val coll = scope.getRowCollection(ref)
      assert(coll.readField(0, 1).value contains "NASDAQ")
      assert(coll.readField(0, 2).value.collect { case r: MultiTenantRowCollection => r.toMapGraph }
        .contains(List(Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")))))
    }

    it("should retrieve rows from a multi-tenant inner-table") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |select * from $ref where symbol is 'SHMN'
            |""".stripMargin))
      assert(device.toMapGraph == List(
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should delete rows from a multi-tenant inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |delete from $ref where __id is 1
            |""".stripMargin))
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should update rows of a multi-tenant inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |update $ref
            |set transactions = [
            |   {"price":35.11, "transactionTime":"2021-08-05T19:23:12.000Z"},
            |   {"price":35.83, "transactionTime":"2021-08-05T19:23:15.000Z"},
            |   {"price":36.03, "transactionTime":"2021-08-05T19:23:17.000Z"},
            |]
            |where symbol is 'AMD'
            |""".stripMargin))
      assert(cost.updated == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should insert embedded rows into a multi-tenant inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |insert into $ref#transactions (price, transactionTime)
            |values (35.11, "2021-08-05T19:23:12.000Z"),
            |       (35.83, "2021-08-05T19:23:15.000Z"),
            |       (36.03, "2021-08-05T19:23:17.000Z")
            |where symbol is 'AMD'
            |""".stripMargin))
      assert(cost.inserted == 8)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 35.11, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z")),
          Map("price" -> 35.83, "transactionTime" -> DateHelper("2021-08-05T19:23:15.000Z")),
          Map("price" -> 36.03, "transactionTime" -> DateHelper("2021-08-05T19:23:17.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should modify embedded rows from a multi-tenant inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |update $ref#transactions
            |set price = 0.0012
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |limit 1
            |""".stripMargin))
      assert(cost.updated == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0012, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should remove embedded rows from a multi-tenant inner-table") {
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|${createStockDataSQL(ref)}
            |delete from $ref#transactions
            |where symbol is 'SHMN'
            |and transactions wherein (price is 0.001)
            |""".stripMargin))
      assert(cost.deleted == 1)
      assert(scope.getRowCollection(ref).toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "INTC", "exchange" -> "NYSE", "transactions" -> List(
          Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ", "transactions" -> List(
          Map("price" -> 988.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))
        )),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB", "transactions" -> List(
          Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))
        ))
      ))
    }

    it("should tables with multiple inner-tables") {
      val ref = DatabaseObjectRef("multiInnerTables")
      val (scope, cost, _) = LollypopVM.executeSQL(Scope(), show(
        s"""|drop if exists $ref
            |create table $ref (
            |   symbol: String(8),
            |   exchange: String(8),
            |   transactions Table (price Double, transactionTime DateTime),
            |   statistics Table (high Double, low Double, volume Double)
            |)
            |
            |insert into $ref (symbol, exchange, transactions, statistics)
            |values ('AAPL', 'NASDAQ', {"price":155.39, "transactionTime":"2021-08-05T19:23:11.000Z"}, {high:158.97, low: 141.55, volume:6754320}),
            |       ('AMD', 'NASDAQ',  {"price":55.87, "transactionTime":"2021-08-05T19:23:11.000Z"}, {high:58.97, low: 41.55, volume:9876543}),
            |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}, {high:89.52, low: 88.11, volume:12345000}),
            |       ('AMZN', 'NASDAQ', {"price":188.12, "transactionTime":"2021-08-05T19:23:11.000Z"}, {high:195.25, low: 188.11, volume:7865432}),
            |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
            |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}],
            |                          [{high:0.0013, low: 0.0009, volume:1000},
            |                           {high:0.0015, low: 0.0012, volume:4500}])
            |""".stripMargin))
      val device = scope.getRowCollection(ref)
      assert(cost.inserted == 5)
      device.tabulate().foreach(logger.info)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAPL", "exchange" -> "NASDAQ",
          "transactions" -> List(Map("price" -> 155.39, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          "statistics" -> List(Map("high" -> 158.97, "low" -> 141.55, "volume" -> 6754320.0))),
        Map("symbol" -> "AMD", "exchange" -> "NASDAQ",
          "transactions" -> List(Map("price" -> 55.87, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          "statistics" -> List(Map("high" -> 58.97, "low" -> 41.55, "volume" -> 9876543.0))),
        Map("symbol" -> "INTC", "exchange" -> "NYSE",
          "transactions" -> List(Map("price" -> 89.44, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          "statistics" -> List(Map("high" -> 89.52, "low" -> 88.11, "volume" -> 1.2345E7))),
        Map("symbol" -> "AMZN", "exchange" -> "NASDAQ",
          "transactions" -> List(Map("price" -> 188.12, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z"))),
          "statistics" -> List(Map("high" -> 195.25, "low" -> 188.11, "volume" -> 7865432.0))),
        Map("symbol" -> "SHMN", "exchange" -> "OTCBB",
          "transactions" -> List(
            Map("price" -> 0.001, "transactionTime" -> DateHelper("2021-08-05T19:23:11.000Z")),
            Map("price" -> 0.0011, "transactionTime" -> DateHelper("2021-08-05T19:23:12.000Z"))),
          "statistics" -> List(
            Map("high" -> 0.0013, "low" -> 9.0E-4, "volume" -> 1000.0),
            Map("high" -> 0.0015, "low" -> 0.0012, "volume" -> 4500.0)))
      ))
    }

  }

  private def createPassengerData(ref: DatabaseObjectRef, textType: String): String = {
    s"""|drop if exists $ref
        |create table $ref (
        |   id RowNumber,
        |   lastName $textType,
        |   firstName $textType,
        |   age Int,
        |   airportCode String(3) = 'LAX'
        |)
        |
        |insert into $ref (lastName, firstName, age, airportCode)
        |values ("JONES", "GARRY", 31, "SNA"),
        |       ("JONES", "TAMERA", 28, "SNA"),
        |       ("JONES", "ERIC", 5, "SNA"),
        |       ("ADAMS", "KAREN", 58, "LAX"),
        |       ("ADAMS", "MIKE", 55, "LAX"),
        |       ("DAVIS", "CHRIS", 39, "DTW")
        |""".stripMargin
  }

  private def createStockDataSQL(ref: DatabaseObjectRef, capacity: Int = 0, isPointer: Boolean = false): String = {
    s"""|drop if exists $ref
        |create table $ref (
        |   symbol: String(8),
        |   exchange: String(8),
        |   transactions Table (
        |       price Double,
        |       transactionTime DateTime
        |   )${if (capacity > 0) s"[$capacity]" else ""}${if (isPointer) "*" else ""}
        |)
        |
        |insert into $ref (symbol, exchange, transactions)
        |values ('AAPL', 'NASDAQ', {"price":155.39, "transactionTime":"2021-08-05T19:23:11.000Z"}),
        |       ('AMD', 'NASDAQ',  {"price":55.87, "transactionTime":"2021-08-05T19:23:11.000Z"}),
        |       ('INTC','NYSE',    {"price":89.44, "transactionTime":"2021-08-05T19:23:11.000Z"}),
        |       ('AMZN', 'NASDAQ', {"price":988.12, "transactionTime":"2021-08-05T19:23:11.000Z"}),
        |       ('SHMN', 'OTCBB', [{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},
        |                          {"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}])
        |""".stripMargin
  }

  private def show(sql: String): String = {
    logger.info(sql)
    sql
  }

}
