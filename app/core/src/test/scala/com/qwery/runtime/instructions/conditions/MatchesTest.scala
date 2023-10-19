package com.qwery.runtime.instructions.conditions

import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class MatchesTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[Matches].getSimpleName) {

    it("should evaluate simple expressions") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|set isNumeric = (o: object) => o.isNumber()
           |5678 matches isNumeric
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
           |val isExchange = (s: String) => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response matches { id: isNumber, symbol: "DOG", exchange: isString, lastSale: isNumber }
           |""".stripMargin)
      assert(result == false)
    }

    it("should evaluate an array of JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response matches [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate an array of JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response matches [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == false)
    }

  }

}
