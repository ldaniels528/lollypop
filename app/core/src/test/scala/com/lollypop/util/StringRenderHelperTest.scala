package com.lollypop.util

import com.lollypop.util.CodecHelper.serialize
import com.lollypop.util.StringRenderHelper.StringRenderer
import org.scalatest.funspec.AnyFunSpec
import lollypop.io.{IOCost, RowIDRange}

import java.util.UUID

/**
 * String Render Helper Test Suite
 */
class StringRenderHelperTest extends AnyFunSpec {

  describe(classOf[StringRenderHelper.type].getSimpleName) {

    it("should render a Byte Array") {
      val result = StringRenderHelper.toByteArrayString("The\nquick\tbrown fox!".getBytes())
      assert(result == """[0x54, 0x68, 0x65, '\n', 0x71, 0x75, 0x69, 0x63, 0x6b, '\t', 0x62, 0x72, 0x6f, 0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x21]""")
    }

    it("should (pretty) render a Byte Array") {
      val result = StringRenderHelper.toByteArrayString("The\nquick\tbrown fox!".getBytes(), isPretty = true)
      assert(result == """["The", '\n', "quick", '\t', "brown fox!"]""")
    }

    it("should (pretty) render a Byte Array containing non-printable characters") {
      val result = StringRenderHelper.toByteArrayString(serialize(UUID.fromString("4ea188b9-4848-4723-817e-6fa063ceeddf")), isPretty = true)
      assert(result == """[0xac, 0xed, 0x00, 0x05, "sr", 0x00, 0x0e, "java.util.UUID", 0xbc, 0x99, 0x03, 0xf7, 0x98, "m", 0x85, "/", 0x02, 0x00, 0x02, "J", 0x00, 0x0c, "leastSigBitsJ", 0x00, 0x0b, "mostSigBitsxp", 0x81, "~o", 0xa0, "c", 0xce, 0xed, 0xdf, "N", 0xa1, 0x88, 0xb9, "HHG#"]""".stripMargin)
    }

    it("should render an Array") {
      val array = Array("Hello", "World")
      assert(array.render == """["Hello", "World"]""")
    }

    it("should render a Date") {
      assert(DateHelper.from(1631508164812L).render == "2021-09-13T04:42:44.812Z")
    }

    it("should render a Double") {
      assert(55.11.render == "55.11")
    }

    it("should render an Int") {
      assert(7779311.render == "7779311")
    }

    it("should render a Long") {
      assert(1234567890L.render == "1234567890")
    }

    it("should render a None") {
      assert(None.render == "None")
    }

    it("should renderAsJson a Map") {
      val map = Map("exchange" -> "NYSE", "symbol" -> "ABC", "lastSale" -> 56.98, "lastSaleTime" -> DateHelper.from(1631508164812L))
      assert(map.renderAsJson ==
        """|{"exchange": "NYSE", "symbol": "ABC", "lastSale": 56.98, "lastSaleTime": "2021-09-13T04:42:44.812Z"}
           |""".stripMargin.trim)
    }

    it("should renderAsJson a nested Map") {
      val map = Map(
        "exchange" -> "NYSE",
        "symbol" -> "ABC",
        "prices" -> List(
          Map("lastSale" -> 56.98, "lastSaleTime" -> DateHelper.from(1631508164812L)),
          Map("lastSale" -> 56.97, "lastSaleTime" -> DateHelper.from(1631508163675L))
        ))
      assert(map.renderAsJson ==
        """|{"exchange": "NYSE", "symbol": "ABC", "prices": [{"lastSale": 56.98, "lastSaleTime": "2021-09-13T04:42:44.812Z"}, {"lastSale": 56.97, "lastSaleTime": "2021-09-13T04:42:43.675Z"}]}
           |""".stripMargin.trim)
    }

    it("should renderAsJson a Product") {
      val cost = IOCost(created = 1, inserted = 5, scanned = 3, matched = 1, rowIDs = new RowIDRange(from = 0, to = 4))
      assert(cost.renderAsJson == """{"shuffled": 0, "rowIDs": [0, 1, 2, 3, 4], "matched": 1, "updated": 0, "destroyed": 0, "scanned": 3, "inserted": 5, "altered": 0, "deleted": 0, "created": 1}""")
    }

    it("should renderAsJson a 2D Tuple") {
      assert((55.11, 10).renderAsJson == "(55.11, 10)")
    }

    it("should renderAsJson a 3D Tuple") {
      assert(("Hello", "World", 84.1234).renderAsJson == """("Hello", "World", 84.1234)""")
    }

    it("should renderAsJson a 6D Tuple") {
      assert((123, "Hello", 'W', Array(1, '2', "3"), 84.1234, DateHelper.from(1631508164812L)).renderAsJson ==
        """(123, "Hello", 'W', [1, '2', "3"], 84.1234, "2021-09-13T04:42:44.812Z")""")
    }

    it("should renderPretty a nested Map") {
      val map = Map(
        "exchange" -> "NYSE",
        "symbol" -> "ABC",
        "prices" -> List(
          Map("lastSale" -> 56.98, "lastSaleTime" -> DateHelper.from(1631508164812L)),
          Map("lastSale" -> 56.97, "lastSaleTime" -> DateHelper.from(1631508163675L))
        ))
      assert(map.renderPretty ==
        """|{"exchange": "NYSE", "symbol": "ABC", "prices": [{"lastSale": 56.98, "lastSaleTime": "2021-09-13T04:42:44.812Z"}, {"lastSale": 56.97, "lastSaleTime": "2021-09-13T04:42:43.675Z"}]}
           |""".stripMargin.trim)
    }

    it("should renderPretty a Product") {
      val cost = IOCost(created = 1, inserted = 5, scanned = 3, matched = 1, rowIDs = new RowIDRange(from = 0, to = 4))
      assert(cost.renderPretty == """IOCost(altered=0, created=1, destroyed=0, deleted=0, inserted=5, matched=1, scanned=3, shuffled=0, updated=0, rowIDs={"start": Some(0), "end": Some(4)})""")
    }

  }

}
