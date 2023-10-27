package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class MatchesTest extends AnyFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[Matches].getSimpleName) {

    it("""should evaluate true: matches("hello", "h.*llo")""") {
      assert(Matches("hello", "h.*llo").isTrue)
    }

    it("""should evaluate false: matches("hello!", "h.*llo")""") {
      assert(Matches("hello!", "h.*llo").isFalse)
    }

    it("""should negate: matches(a, "hello")""") {
      assert(Matches("a".f, "hello").negate == Not(Matches("a".f, "hello")))
    }

    it("""should negate: not(matches(a, "hello"))""") {
      assert(Not(Matches("a".f, "hello")).negate == Matches("a".f, "hello"))
    }

    it("""should decompile: matches(name, "Lawr.*")""") {
      assert(Matches("name".f, "Lawr.*").toSQL == """name matches "Lawr.*"""")
    }

    it("should evaluate: 'Hello World' matches 'H.* W.*'") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|"Hello World" matches "H.* W.*"
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate: 5678 matches isNumeric") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|isNumeric = x => x.isNumber()
           |5678 matches isNumeric
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
           |val isExchange = s => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
           |val isNumber = x => x.isNumber()
           |val isString = x => x.isString()
           |response matches { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }
           |val isNumber = x => x.isNumber()
           |val isString = x => x.isString()
           |response matches { id: isNumber, symbol: "DOG", exchange: isString, lastSale: isNumber }
           |""".stripMargin)
      assert(result == false)
    }

    it("should evaluate an array of JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = x => x.isNumber()
           |val isString = x => x.isString()
           |response matches [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate an array of JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = x => x.isNumber()
           |val isString = x => x.isString()
           |response matches [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == false)
    }

  }

}
