package com.lollypop.util

import com.lollypop.language.LollypopUniverse
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.JSONSupport.JSONProductConversion
import com.lollypop.util.JSONSupportTest.{Ticker, TickerPojo}
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import spray.json.{JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue}

import scala.beans.BeanProperty
import scala.collection.mutable

/**
 * JSON Support Test Suite
 * @author lawrence.daniels@gmail.com
 */
class JSONSupportTest extends AnyFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()

  describe(JSONSupport.getClass.getSimpleName.replaceAll("[$]", "")) {

    it("should convert a BigDecimal to a JSON value") {
      val number = 133.0
      verify(item = BigDecimal(number), expected = JsNumber(number))
    }

    it("should convert a BigInt to a JSON value") {
      val number = 0xDEADBEEFL
      verify(item = BigInt(number), expected = JsNumber(number))
    }

    it("should convert a Boolean to a JSON value") {
      verify(item = true, expected = JsTrue)
    }

    it("should convert a Date to a JSON value") {
      val date = "2021-05-17T11:20:51.667Z"
      verify(item = DateHelper.parse(date), expected = JsString(date))
    }

    it("should convert a Number to a JSON value") {
      val number = 77793111
      verify(item = number, expected = JsNumber(number))
    }

    it("should convert a String to a JSON value") {
      val value = "Hello World"
      verify(item = value, expected = JsString(value))
    }

    it("should convert an Array to a JSON array") {
      verify(
        item = Array("Hello World", true, 77793111),
        expected = JsArray(JsString("Hello World"), JsTrue, JsNumber(77793111)))
    }

    it("should convert a List (primitives) to a JSON array") {
      verify(
        item = List(new mutable.StringBuilder("Hello World"), false, 341.75),
        expected = JsArray(JsString("Hello World"), JsFalse, JsNumber(341.75)))
    }

    it("should convert a List (objects) to a JSON array") {
      verify(
        item = List(
          Ticker(symbol = "ABC", exchange = "NYSE", lastSale = 13.87),
          Ticker(symbol = "MEAT.OB", exchange = "OTCBB", lastSale = 0.0001),
          Ticker(symbol = "TREE", exchange = "NASDAQ", lastSale = 56.79)
        ),
        expected = JsArray(
          JsObject(Map("symbol" -> JsString("ABC"), "exchange" -> JsString("NYSE"), "lastSale" -> JsNumber(13.87), "_class" -> JsString("com.lollypop.util.JSONSupportTest$Ticker"))),
          JsObject(Map("symbol" -> JsString("MEAT.OB"), "exchange" -> JsString("OTCBB"), "lastSale" -> JsNumber(0.0001), "_class" -> JsString("com.lollypop.util.JSONSupportTest$Ticker"))),
          JsObject(Map("symbol" -> JsString("TREE"), "exchange" -> JsString("NASDAQ"), "lastSale" -> JsNumber(56.79), "_class" -> JsString("com.lollypop.util.JSONSupportTest$Ticker")))
        ))
    }

    it("should convert a Map to a JSON value") {
      verify(
        item = Map("symbol" -> "ABC", "exchange" -> "NASDAQ", "lastSale" -> 56.79),
        expected = JsObject(Map("symbol" -> JsString("ABC"), "exchange" -> JsString("NASDAQ"), "lastSale" -> JsNumber(56.79)
        )))
    }

    it("should select an adhoc JSON object") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|select dict: {
           |  symbol: "GMTQ",
           |  exchange: "OCTBB",
           |  lastSale: 0.1111,
           |  lastSaleTime: DateTime(1631508164812)
           |}
           |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("dict" -> Map(
          "symbol" -> "GMTQ",
          "exchange" -> "OCTBB",
          "lastSale" -> 0.1111,
          "lastSaleTime" -> new java.util.Date(1631508164812L)
        ))
      ))
    }

    it("should convert a Table to a JSON array") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|declare table stockQuotes (
           |   symbol: String(8),
           |   exchange: String(8),
           |   transactions Table (
           |       price Double,
           |       transactionTime DateTime
           |   )[3]
           |)[3]
           |insert into @stockQuotes (symbol, exchange, transactions)
           |values ('AAPL', 'NASDAQ', '{"price":156.39, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
           |       ('AMD', 'NASDAQ', '{"price":56.87, "transactionTime":"2021-08-05T19:23:11.000Z"}'),
           |       ('SHMN', 'OTCBB', '[{"price":0.0010, "transactionTime":"2021-08-05T19:23:11.000Z"},{"price":0.0011, "transactionTime":"2021-08-05T19:23:12.000Z"}]')
           |stockQuotes
           |""".stripMargin
      )
      assert(device.toJsValue.toString() ==
        """|[{"symbol":"AAPL","exchange":"NASDAQ","transactions":[{"price":156.39,"transactionTime":"2021-08-05T19:23:11.000Z"}]},
           |{"symbol":"AMD","exchange":"NASDAQ","transactions":[{"price":56.87,"transactionTime":"2021-08-05T19:23:11.000Z"}]},
           |{"symbol":"SHMN","exchange":"OTCBB","transactions":[{"price":0.001,"transactionTime":"2021-08-05T19:23:11.000Z"},
           |{"price":0.0011,"transactionTime":"2021-08-05T19:23:12.000Z"}]}]
           |""".stripMargin.replaceAll("\n", "")
      )
    }

    it("should convert a case class to a JSON object") {
      verify(
        item = Ticker(symbol = "TREE", exchange = "NASDAQ", lastSale = 56.79),
        expected = JsObject(Map(
          "symbol" -> JsString("TREE"), "exchange" -> JsString("NASDAQ"), "lastSale" -> JsNumber(56.79),
          "_class" -> JsString("com.lollypop.util.JSONSupportTest$Ticker")
        )))
    }

    it("should convert a POJO class to a JSON object") {
      verify(
        item = new TickerPojo(symbol = "TREE", exchange = "NASDAQ", lastSale = 56.79),
        expected = JsObject(Map(
          "symbol" -> JsString("TREE"),
          "exchange" -> JsString("NASDAQ"),
          "lastSale" -> JsNumber(56.79),
          "_class" -> JsString("com.lollypop.util.JSONSupportTest$TickerPojo")
        )))
    }

  }

  private def verify(item: Any, expected: JsValue): Assertion = {
    val actual = item.toJsValue
    if (actual != expected) info(s"actual: $actual")
    info(s"expected: $expected")
    assert(actual == expected)
  }

}

object JSONSupportTest {

  case class Ticker(symbol: String, exchange: String, lastSale: Double)

  class TickerPojo(@BeanProperty val symbol: String,
                   @BeanProperty val exchange: String,
                   @BeanProperty val lastSale: Double)

}