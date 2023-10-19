package com.qwery.runtime.instructions.functions

import com.qwery.language.TokenStream
import com.qwery.language.models.Expression
import com.qwery.language.models.Expression.implicits.LifestyleExpressionsAny
import com.qwery.runtime.datatypes._
import com.qwery.runtime.instructions.expressions.NamedFunctionCall
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import com.qwery.util.DateHelper
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import qwery.lang.Pointer

import java.util.UUID
import scala.concurrent.duration.DurationInt

/**
 * Native Type Constructor Tests
 */
class NamedFunctionCallTest extends AnyFunSpec {
  private implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[NamedFunctionCall].getSimpleName) {

    it("should compile: Boolean('True')") {
      verify("Boolean(\"True\")", NamedFunctionCall("Boolean", args = List("True".v)))
    }

    it("should decompile: Boolean('True')") {
      eval("Boolean('True')", expect = true)
    }

    it("should compile: Char('T')") {
      verify("Char('T')", NamedFunctionCall("Char", args = List('T'.v)))
    }

    it("should decompile: Char('T')") {
      eval("Char('T')", expect = 'T')
    }

    it("""should compile: Float("8765.4321")""") {
      verify("""Float("8765.4321")""", Float32Type("8765.4321".v))
    }

    it("""should decompile: Float("8765.4321")""") {
      eval("""Float("8765.4321")""", expect = 8765.4321f)
    }

    it("""should compile: Double(123456.7890)""") {
      verify("""Double("123456.7890")""", Float64Type("123456.7890".v))
    }

    it("""should decompile: Double(123456.7890)""") {
      eval("""Double("123456.7890")""", expect = 123456.7890)
    }

    it("""should compile: Byte("21")""") {
      verify("""Byte("21")""", Int8Type("21".v))
    }

    it("""should decompile: Byte("21")""") {
      eval("""Byte("21")""", expect = 21)
    }

    it("""should compile: Short("24321")""") {
      verify("""Short("24321")""", Int16Type("24321".v))
    }

    it("""should decompile: Short("24321")""") {
      eval("""Short("24321")""", expect = 24321)
    }

    it("""should compile: Int("87654321")""") {
      verify("""Int("87654321")""", Int32Type("87654321".v))
    }

    it("""should decompile: Int("87654321")""") {
      eval("""Int("87654321")""", expect = 87654321)
    }

    it("""should compile: Long(1234567890)""") {
      verify("""Long("1234567890")""", Int64Type("1234567890".v))
    }

    it("""should decompile: Long(1234567890)""") {
      eval("""Long("1234567890")""", expect = 1234567890L)
    }

    it("""should compile: Numeric(123456.7890)""") {
      verify("""Numeric("123456.7890")""", NumericType("123456.7890".v))
    }

    it("""should decompile: Numeric(123456.7890)""") {
      eval("""Numeric("123456.7890")""", expect = BigDecimal(123456.7890))
    }

    it("""should compile: Interval("7 DAYS")""") {
      verify("""Interval("7 DAYS")""", IntervalType("7 DAYS".v))
    }

    it("""should decompile: Interval("7 DAYS")""") {
      eval("""Interval("7 DAYS")""", expect = 7.days)
    }

    it("""should compile: Pointer(0, 23, 1024)""") {
      val model = compiler.nextExpression(TokenStream("Pointer(0, 23, 1024)"))
      assert(model contains NamedFunctionCall("Pointer", args = List(0.v, 23.v, 1024.v)))
    }

    it("""should decompile: Pointer(0, 23, 1024)""") {
      eval("""Pointer(0, 23, 1024)""", expect = Pointer(0, 23, 1024))
    }

    it("""should compile: String("Hello World")""") {
      verify("""String("Hello World")""", NamedFunctionCall("String", args = List("Hello World".v)))
    }

    it("""should decompile: String("Hello World")""") {
      eval("""String("Hello World")""", expect = "Hello World")
    }

    it("""should compile: DateTime("2021-09-02T11:22:33.000Z")""") {
      verify("""DateTime("2021-09-02T11:22:33.000Z")""", DateTimeType("2021-09-02T11:22:33.000Z".v))
    }

    it("""should decompile: DateTime("2021-09-02T11:22:33.000Z")""") {
      eval("""DateTime("2021-09-02T11:22:33.000Z")""", expect = DateHelper.parse("2021-09-02T11:22:33.000Z"))
    }

    it("""should compile: UUID("363a967a-df2b-452c-aab6-ca1d1c0ac24b")""") {
      verify("UUID()", NamedFunctionCall("UUID", args = Nil))
    }

    it("""should decompile: UUID("363a967a-df2b-452c-aab6-ca1d1c0ac24b")""") {
      verify("""UUID("363a967a-df2b-452c-aab6-ca1d1c0ac24b")""", NamedFunctionCall("UUID", args = List("363a967a-df2b-452c-aab6-ca1d1c0ac24b".v)))
    }

    it("""should evaluate: UUID("363a967a-df2b-452c-aab6-ca1d1c0ac24b")""") {
      eval("""UUID("363a967a-df2b-452c-aab6-ca1d1c0ac24b")""", expect = UUID.fromString("363a967a-df2b-452c-aab6-ca1d1c0ac24b"))
    }

  }

  private def verify(sql: String, model: Expression): Assertion = {
    compileAndVerify(sql, model)
    decompileAndVerify(model, sql)
  }

  private def eval(sql: String, expect: Any): Assertion = {
    val (_, _, result) = QweryVM.executeSQL(Scope(), sql)
    assert(result == expect)
  }

  private def compileAndVerify(expr: String, expect: Expression): Assertion = {
    val actual = compiler.nextExpression(TokenStream(expr))
    info(s"$expr => $actual")
    assert(actual.contains(expect), s"$expr : failed")
  }

  private def decompileAndVerify(expr: Expression, expect: String): Assertion = {
    val actual = expr.toSQL
    info(s"$expr => $actual")
    assert(actual == expect, s"${expr.toSQL} : failed")
  }

}
