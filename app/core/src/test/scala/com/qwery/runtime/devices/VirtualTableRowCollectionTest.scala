package com.qwery.runtime.devices

import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, ResourceManager, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory
import qwery.io.IOCost

import scala.io.Source

/**
 * Virtual Table Row Collection Test
 */
class VirtualTableRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val tableRef: DatabaseObjectRef = DatabaseObjectRef(getTestTableName)
  private val viewRef: DatabaseObjectRef = DatabaseObjectRef(s"${getTestTableName}_50")

  describe(classOf[VirtualTableRowCollection].getName) {

    it("should prepare a sample data table") {
      val (scope, cost) = QweryVM.infrastructureSQL(Scope(),
        s"""|drop if exists $tableRef
            |create table $tableRef (
            |   symbol: String(5),
            |   exchange: String(6),
            |   lastSale: Double,
            |   lastSaleTime: DateTime
            |)
            |""".stripMargin)
      assert(cost == IOCost(created = 1, destroyed = 1))

      // insert 5,000 records
      val src = Source.fromFile("./contrib/examples/stocks-5k.csv")
      val lines = src.getLines()
      lines.next()
      lines foreach { line =>
        if (line.trim.nonEmpty) {
          val Array(symbol, exchange, lastSale, lastSaleTime) = line.split("[,]")
          val (_, cost) = QweryVM.infrastructureSQL(scope,
            s"""|insert into $tableRef (symbol, exchange, lastSale, lastSaleTime)
                |values ($symbol, $exchange, $lastSale, $lastSaleTime)
                |""".stripMargin)
          assert(cost.inserted == 1)
        }
      }
      src.close()
    }

    it("should perform create view") {
      val (_, cost) = QweryVM.infrastructureSQL(Scope(),
        s"""|drop if exists $viewRef &&
            |create view $viewRef
            |as
            |select
            |   symbol,
            |   exchange,
            |   lastSale,
            |   lastSaleTime
            |from $tableRef
            |where lastSale < 50
            |order by lastSale desc
            |limit 50
            |""".stripMargin)
      assert(cost.created == 1)
    }

    it("should pull records from the view") {
      val (scope, cost, results) = QweryVM.searchSQL(Scope(),
        s"""|select symbol, exchange, lastSale from $viewRef where symbol is 'SBYY'
            |""".stripMargin)
      cost.toTable(scope).tabulate().foreach(logger.info)
      //assert(cost == IOCost(matched = 1, scanned = 1))
      assert(results.toMapGraph == List(Map("symbol" -> "SBYY", "exchange" -> "OTCBB", "lastSale" -> 0.4246)))
    }

    it("should accelerate queries via an index on the view") {
      implicit val scope: Scope = Scope()

      // remove the view from cache
      ResourceManager.close(viewRef.toNS)

      val (_, cost1) = QweryVM.infrastructureSQL(scope,
        s"""|drop if exists $viewRef#symbol
            |create index $viewRef#symbol
            |""".stripMargin)
      assert(cost1 == IOCost(created = 1, destroyed = 1, shuffled = 50))

      val (_, cost2, results2) = QweryVM.searchSQL(scope,
        s"""|select symbol, exchange, lastSale from $viewRef where symbol is 'SBYY'
            |""".stripMargin)
      assert(cost2 == IOCost(matched = 1, scanned = 1))
      assert(results2.toMapGraph == List(Map("symbol" -> "SBYY", "exchange" -> "OTCBB", "lastSale" -> 0.4246)))
    }

  }

}