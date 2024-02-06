package com.lollypop.runtime.devices

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.DatabaseObjectRef.DatabaseObjectRefRealization
import com.lollypop.runtime._
import com.lollypop.runtime.datatypes.{Float64Type, StringType}
import com.lollypop.runtime.devices.RemoteRowCollection.getRemoteCollection
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.VerificationTools.closeOnShutdown
import lollypop.io.{IOCost, Nodes, RowIDRange}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class RemoteRowCollectionTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  private val ctx = LollypopUniverse(isServerMode = true)
  private val rootScope = ctx.createRootScope()
  private val node = new Nodes(ctx).start()
  private val port = node.server.port
  private val ref = DatabaseObjectRef(getTestTableName)

  closeOnShutdown(node)

  describe(classOf[RemoteRowCollection].getSimpleName) {
    val (scope, _, _) = LollypopVM.searchSQL(rootScope,
      s"""|drop if exists $ref
          |create table $ref (
          |   symbol: String(7),
          |   exchange: String(5),
          |   lastSale: Double,
          |   code: String(2)
          |)
          |
          |insert into $ref (symbol, exchange, lastSale, code)
          |values
          |  ('AAXX', 'NYSE', 56.12, 'A'), ('UPEX', 'NYSE', 116.24, 'A'), ('XYZ', 'AMEX', 31.9500, 'A'),
          |  ('JUNK', 'AMEX', 97.61, 'B'), ('RTX.OB', 'OTCBB', 1.93011, 'B'), ('ABC', 'NYSE', 1235.7650, 'B'),
          |  ('UNIB.OB', 'OTCBB', 9.11, 'C'), ('BRT.OB', 'OTCBB', 0.00123, 'C'), ('PLUMB', 'NYSE', 1009.0770, 'C')
          |""".stripMargin)

    val device = RemoteRowCollection(host = "0.0.0.0", port = port, ns = ref.toNS(scope))

    it("should decode remote table URLs") {
      val coll_? = getRemoteCollection(s"//0.0.0.0:$port/$ref")(Scope())
      assert(coll_? contains RemoteRowCollection(
        host = "0.0.0.0",
        port = port,
        ns = ref.toNS(scope)
      ))
      assert(coll_?.toList.flatMap(_.columns) == List(
        TableColumn(name = "symbol", `type` = StringType(7)),
        TableColumn(name = "exchange", `type` = StringType(5)),
        TableColumn(name = "lastSale", `type` = Float64Type),
        TableColumn(name = "code", `type` = StringType(2))
      ))
    }

    it("should retrieve a remote collection as a byte array") {
      assert(device.encode.length == 1040)
    }

    it("should retrieve a row from a remote collection") {
      val row = device.get(0).map(_.toMap)
      assert(row contains Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "code" -> "A"))
    }

    it("should insert a row into a remote collection") {
      val row = Map("symbol" -> "BATZ", "exchange" -> "NYSE", "lastSale" -> 8.16, "code" -> "A").toRow(device)
      val cost = device.insert(row)
      assert(cost == IOCost(inserted = 1, rowIDs = RowIDRange(9L)))
      assert(device.get(9).map(_.toMap) contains row.toMap)
    }

    it("should update a row within a remote collection") {
      val row = Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12, "code" -> "C").toRow(device)
      val cost = device.update(rowID = 0, row)
      assert(cost == IOCost(updated = 1))
      assert(device.get(0) contains row)
    }

    it("should retrieve a field from a remote collection") {
      val field = device.readField(rowID = 0, columnID = 0)
      assert(field.value contains "AAXX")
    }

    it("should update a field within a remote collection") {
      val newSymbol = "DOGMA"
      val cost = device.updateField(rowID = 0, columnID = 0, newValue = Some(newSymbol))
      assert(cost == IOCost(updated = 1))

      val value = device.readField(rowID = 0, columnID = 0).value
      assert(value contains newSymbol)
    }

    it("should retrieve field metadata from a remote collection") {
      val fmd = device.readFieldMetadata(rowID = 0, columnID = 0)
      assert(fmd == FieldMetadata(isActive = true))
    }

    it("should update field metadata within a remote collection") {
      val cost = device.updateFieldMetadata(rowID = 0, columnID = 3, FieldMetadata(isActive = false))
      assert(cost == IOCost(updated = 1))
    }

    it("should retrieve row metadata from a remote collection") {
      val rmd = device.readRowMetadata(rowID = 0)
      assert(rmd == RowMetadata(isAllocated = true))
    }

    it("should update row metadata within a remote collection") {
      val cost = device.updateRowMetadata(rowID = 4, RowMetadata(isAllocated = false))
      assert(cost == IOCost(updated = 1))
    }

    it("should retrieve rows from a remote collection") {
      device.tabulate(limit = 5).foreach(logger.info)
    }

    it("should retrieve the length of a remote collection") {
      assert(device.getLength == 10)
    }

    it("should set the length of a remote collection") {
      assert(device.setLength(8) == IOCost(updated = 1))
      assert(device.getLength == 8)
    }

  }

}
