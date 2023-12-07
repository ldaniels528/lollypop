package com.lollypop.runtime.devices

import com.lollypop.runtime._
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.util.DateHelper
import org.scalatest.funspec.AnyFunSpec

class RowCollectionZooTest extends AnyFunSpec with VerificationTools {

  describe(RowCollectionZoo.getClass.getSimpleName.replaceAll("[$]", "")) {
    val ref = DatabaseObjectRef(getTestTableName)

    it("should convert a product instance (RowSummary) into a table or table type") {
      val (scope0, _, _) = LollypopVM.executeSQL(Scope(),
        s"""|drop if exists $ref
            |create table $ref (symbol: String(8), exchange: String(8), lastSale: Double)
            |insert into $ref (symbol, exchange, lastSale)
            |values ('AAPL', 'NASDAQ', 202.11), ('AMD', 'NYSE', 23.50),
            |       ('GOOG', 'NYSE', 765.33), ('AMZN', 'NASDAQ', 699.01)
            |""".stripMargin)
      val device = scope0.getRowCollection(ref)
      device.delete(1)
      val rowSummary = device.getRowSummary
      assert(rowSummary.toRowCollection.toMapGraph == List(
        Map("replicated" -> 0, "locked" -> 0, "encrypted" -> 0, "compressed" -> 0, "deleted" -> 1, "active" -> 3)
      ))
      assert(rowSummary.toTableType == TableType(columns = Seq(
        TableColumn(name = "active", `type` = Int64Type),
        TableColumn(name = "compressed", `type` = Int64Type),
        TableColumn(name = "deleted", `type` = Int64Type),
        TableColumn(name = "encrypted", `type` = Int64Type),
        TableColumn(name = "locked", `type` = Int64Type),
        TableColumn(name = "replicated", `type` = Int64Type)
      )))
    }

    it("should convert a collection of rows into a table") {
      val layout = RecordStructure(Seq(
        TableColumn(name = "symbol", `type` = StringType(6)),
        TableColumn(name = "exchange", `type` = StringType(6)),
        TableColumn(name = "lastSale", `type` = Float64Type),
        TableColumn(name = "lastSaleTime", `type` = DateTimeType)))
      val stocks = List(
        Map("symbol" -> "BXXG", "exchange" -> "NASDAQ", "lastSale" -> 147.63, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "KFFQ", "exchange" -> "NYSE", "lastSale" -> 22.92, "lastSaleTime" -> DateHelper.from(1596317591000L)),
        Map("symbol" -> "GTKK", "exchange" -> "NASDAQ", "lastSale" -> 240.14, "lastSaleTime" -> DateHelper.from(1596835991000L)),
        Map("symbol" -> "KNOW", "exchange" -> "OTCBB", "lastSale" -> 357.21, "lastSaleTime" -> DateHelper.from(1597872791000L)))
      val rc = stocks.map(_.toRow(rowID = 0L)(layout)).toRowCollection
      assert(rc.toMapGraph == stocks)
    }

  }

}
