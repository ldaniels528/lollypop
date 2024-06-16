package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.{TokenStream, _}
import com.lollypop.language.implicits._
import com.lollypop.language.models.Expression
import com.lollypop.runtime.LollypopCompiler
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ArrayLiteralTest extends AnyFunSpec {
  private val logger = LoggerFactory.getLogger(getClass)
  private implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[ArrayLiteral].getSimpleName) {

    it("should parse an array: [56.87, 1628445935836, '2021-08-05T04:18:30.000Z']") {
      verify("[56.87, 16284, '2021-08-05T04:18:30.000Z']",
        ArrayLiteral(56.87, 16284, "2021-08-05T04:18:30.000Z"))
    }

    it("should parse an array: ['A' to 'Z']") {
      verify("['A' to 'Z']", ArrayLiteral(Span.Inclusive('A', 'Z')))
    }

    it("should parse an array: ['0' to '9'] + ['A' to 'F']") {
      verify("['0' to '9'] + ['A' to 'F']", ArrayLiteral(Span.Inclusive('0', '9')) + ArrayLiteral(Span.Inclusive('A', 'F')))
    }

    it("should parse an array: ['A' until 'Z']") {
      verify("['A' until 'Z']", ArrayLiteral(Span.Exclusive('A', 'Z')))
    }

    it("should parse an array: ['0' until '9'] + ['A' until 'F']") {
      verify("['0' until '9'] + ['A' until 'F']", ArrayLiteral(Span.Exclusive('0', '9')) + ArrayLiteral(Span.Exclusive('A', 'F')))
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
