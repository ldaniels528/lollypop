package com.lollypop.runtime

import com.lollypop.language.implicits._
import com.lollypop.language.models._
import com.lollypop.language.{TokenStream, _}
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.expressions._
import com.lollypop.runtime.instructions.expressions.aggregation.{Max, Min, Sum, Unique}
import lollypop.lang.Null
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

/**
 * Lollypop Compiler Test
 * @author lawrence.daniels@gmail.com
 */
class LollypopCompilerTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[LollypopCompiler].getSimpleName) {

    it("should compile { .. }") {
      assert(compiler.compile("{ }") == CodeBlock())
    }

    it("should decompile { .. }") {
      verify("{ }")
    }

    it("should compile begin .. end") {
      assert(compiler.compile("begin end") == CodeBlock())
    }

    it("should decompile begin .. end") {
      verify("begin end")
    }

    it("should parse an array: [56.87, 1628445935836, '2021-08-05T04:18:30.000Z']") {
      verify("[56.87, 1628445935836, '2021-08-05T04:18:30.000Z']",
        ArrayLiteral(56.87, 1628445935836d, "2021-08-05T04:18:30.000Z"))
    }

    it("should parse an array: ['A' to 'Z']") {
      verify("['A' to 'Z']", ArrayFromRange.Inclusive('A', 'Z'))
    }

    it("should parse an array: ['0' to '9'] + ['A' to 'F']") {
      verify("['0' to '9'] + ['A' to 'F']", ArrayFromRange.Inclusive('0', '9') + ArrayFromRange.Inclusive('A', 'F'))
    }

    it("should parse an array: ['A' until 'Z']") {
      verify("['A' until 'Z']", ArrayFromRange.Exclusive('A', 'Z'))
    }

    it("should parse an array: ['0' until '9'] + ['A' until 'F']") {
      verify("['0' until '9'] + ['A' until 'F']", ArrayFromRange.Exclusive('0', '9') + ArrayFromRange.Exclusive('A', 'F'))
    }

    it("should parse an array index: items(5)") {
      verify("items(5)", "items".fx(5))
    }

    it("should parse an array index: items(n)") {
      verify("items(n)", "items".fx("n".f))
    }

    it("should parse an array index: ['NW', 'NE'](1)") {
      verify("['NW', 'NE'](1)", ApplyTo(ArrayLiteral("NW", "NE"), 1))
    }

    it("should parse an array index: items[5]") {
      verify("items[5]", ElementAt("items".f, 5))
    }

    it("should parse an array index: items[n]") {
      verify("items[n]", ElementAt("items".f, "n".f))
    }

    it("should parse an array index: ['NW', 'NE'][1]") {
      verify("['NW', 'NE'][1]", ElementAt(ArrayLiteral("NW", "NE"), 1))
    }

    it("should parse a dictionary entry: size: 5") {
      verify("size: 5", 5.v.as("size"))
    }

    it("should parse a dictionary: { type: 'numbers', items: [1, 2, 5] } ") {
      verify("{ type: 'numbers', items: [1, 2, 5] }", Dictionary("type" -> "numbers", "items" -> ArrayLiteral(1, 2, 5)))
    }

    it("should parse a dictionary infix operation: { A: { total : 100 } }.A.total ") {
      verify("({ A: { total : 100 } }.A).total", Infix(Infix(Dictionary("A" -> Dictionary("total" -> 100.v)), "A".f), "total".f))
    }

    it("""should parse "unique(PROPERTY_VAL)" """) {
      verify("unique(PROPERTY_VAL)", Unique("PROPERTY_VAL".f))
    }

    it("""should parse "unique(A.Symbol, A.Exchange)" """) {
      verify("unique(A.Symbol, A.Exchange)", Unique(List(Infix("A".f, "Symbol".f), Infix("A".f, "Exchange".f))))
    }

    it("""should parse "A.Symbol" (expression)""") {
      verify("A.Symbol", Infix("A".f, "Symbol".f))
    }

    it("""should parse "Symbol in [100 to 105]" """) {
      verify("Symbol in [100 to 105]", IN("Symbol".f, ArrayFromRange.Inclusive(100, 105)))
    }

    it("""should parse "Symbol in ['AAPL', 'AMZN', 'AMD']" """) {
      verify("Symbol in ['AAPL', 'AMZN', 'AMD']", IN("Symbol".f, ArrayLiteral("AAPL", "AMZN", "AMD")))
    }

    it("""should parse conditional expression "100 < 1" (conditional expression)""") {
      verify("100 < 1", Literal(100) < 1)
    }

    it("""should parse "'Hello World' == 'Goodbye'" (conditional expression)""") {
      verify("'Hello World' == 'Goodbye'", Literal("Hello World") === "Goodbye")
    }

    it("""should parse "`Symbol` == 'AAPL'" (conditional expression)""") {
      verify("`Symbol` == 'AAPL'", "Symbol".f === "AAPL")
    }

    it("""should parse "A.Symbol == 'AMD'" (conditional expression)""") {
      verify("A.Symbol == 'AMD'", Infix("A".f, "Symbol".f) === "AMD")
    }

    it("""should parse "y + (x * 2)" (expression)""") {
      verify("y + (x * 2)", "y".f + ("x".f * 2))
    }

    it("""should parse "y + (x / 2)" (expression)""") {
      verify("y + (x / 2)", "y".f + ("x".f / 2))
    }

    it("""should parse "(y - (x / 2)) as calc" (expression)""") {
      verify("(y - (x / 2))  as calc", "y".f - ("x".f / 2).as("calc"))
    }

    it("""should parse "LastSale == 100" (equal)""") {
      verify("LastSale == 100", "LastSale".f === 100)
    }

    it("""should parse "LastSale != 101" (not equal)""") {
      verify("LastSale != 101", "LastSale".f !== 101)
    }

    it("""should parse "LastSale != 102" (not equal)""") {
      verify("LastSale != 102", "LastSale".f !== 102)
    }

    it("""should parse "not LastSale = 103" (not equal)""") {
      verify("not LastSale == 103", Not("LastSale".f === 103))
    }

    it("""should parse "LastSale > 104" (greater)""") {
      verify("LastSale > 104", "LastSale".f > 104)
    }

    it("""should parse "LastSale >= 105" (greater or equal)""") {
      verify("LastSale >= 105", "LastSale".f >= 105)
    }

    it("""should parse "LastSale < 106" (lesser)""") {
      verify("LastSale < 106", "LastSale".f < 106)
    }

    it("""should parse "LastSale <= 107" (lesser or equal)""") {
      verify("LastSale <= 107", "LastSale".f <= 107)
    }

    it("""should parse "Sector is null" (is null)""") {
      verify("Sector is null", Is("Sector".f, Null()))
    }

    it("""should parse "Sector isnt null" (isnt null)""") {
      verify("Sector isnt null", Isnt("Sector".f, Null()))
    }

    it("""should parse expressions containing 'and'""") {
      verify("Sector == 'Basic Industries' and Industry == 'Gas & Oil'",
        AND("Sector".f === "Basic Industries", "Industry".f === "Gas & Oil"))
    }

    it("""should parse expressions containing 'or'""") {
      verify("Sector == 'Basic Industries' or Industry == 'Gas & Oil'",
        OR("Sector".f === "Basic Industries", "Industry".f === "Gas & Oil"))
    }

    it("""should parse expressions containing 'and' and 'or'""") {
      verify("Sector == 'Basic Industries' and (Industry matches '%Gas%' or Industry matches '%Oil%')",
        AND("Sector".f === "Basic Industries", OR(Matches("Industry".f, "%Gas%"), Matches("Industry".f, "%Oil%"))))
    }

    it("""should parse "(x + 3) * 2" (quantities)""") {
      verify("(x + 3) * 2", ("x".f + 3) * 2)
    }

    it("should parse multi-line double-quotes strings (\"\"\")") {
      verify("\"\"\"\"Hello World\"\"\"\"", "\"Hello World\"".v)
    }

    it("should parse functions (min)") {
      verify("min(LastSale)", Min("LastSale".f))
    }

    it("should parse functions (max)") {
      verify("max(LastSale)", Max("LastSale".f))
    }

    it("should parse functions (sum)") {
      verify("sum(LastSale)", Sum("LastSale".f))
    }

    it("should parse local variables: \"total\"") {
      verify("total", "total".f)
    }

  }

  private def verify(expr: String, expect: Expression): Unit = {
    val actual = compiler.nextExpression(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

  private def verify(expectedSQL: String): Assertion = {
    logger.info(s"expected: $expectedSQL")
    val expected = compiler.compile(expectedSQL)
    val actualSQL = expected.toSQL
    logger.info(s"actual: $actualSQL")
    val actual = compiler.compile(actualSQL)
    assert(expected == actual)
  }

}
