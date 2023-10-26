package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression.implicits._
import com.qwery.runtime.{QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class LikeTest extends AnyFunSpec {
  implicit val scope: Scope = Scope()

  describe(classOf[Like].getSimpleName) {

    it("""should evaluate true: like("hello", "h%llo")""") {
      assert(Like("hello", "h%llo").isTrue)
    }

    it("""should evaluate false: like("hello!", "h%llo")""") {
      assert(Like("hello!", "h%llo").isFalse)
    }

    it("""should negate: like(a, "hello")""") {
      assert(Like("a".f, "hello").negate == Not(Like("a".f, "hello")))
    }

    it("""should negate: not(like(a, "hello"))""") {
      assert(Not(Like("a".f, "hello")).negate == Like("a".f, "hello"))
    }

    it("""should decompile: like(name, "Lawr%")""") {
      assert(Like("name".f, "Lawr%").toSQL == """name like "Lawr%"""")
    }

    it("should evaluate: 'Hello World' like 'H% W%'") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|"Hello World" like "H% W%"
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate: 5678 like isNumeric") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|isNumeric = (o: object) => o.isNumber()
           |5678 like isNumeric
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: 5678, symbol: "DOG", exchange: "NYSE", lastSale: 90.67 }
           |val isExchange = (s: String) => s in ['NYSE', 'AMEX', 'NASDAQ', 'OTCBB']
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response like { id: isNumber, symbol: isString, exchange: isExchange, lastSale: isNumber }
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = { id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response like { id: isNumber, symbol: "DOG", exchange: isString, lastSale: isNumber }
           |""".stripMargin)
      assert(result == false)
    }

    it("should evaluate an array of JSON objects (match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: 5678, symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response like [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == true)
    }

    it("should evaluate an array of JSON objects (no match)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|val response = [{ id: "5678", symbol: "DOG", exchange: "NYSE", "lastSale": 90.67 }]
           |val isNumber = (o: Any) => o.isNumber()
           |val isString = (o: Any) => o.isString()
           |response like [{ id: isNumber, symbol: isString, exchange: isString, lastSale: isNumber }]
           |""".stripMargin)
      assert(result == false)
    }

  }

}
