package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.instructions.expressions.ArrayLiteral
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

import scala.util.{Failure, Success, Try}

class HashTagTest extends AnyFunSpec {

  describe(classOf[HashTag].getSimpleName) {

    it("should compile: stocks#symbol") {
      assert(LollypopCompiler().compile("stocks#symbol") == HashTag("stocks".f, "symbol".f))
    }

    it("should compile: stocks#[symbol, exchange]") {
      assert(LollypopCompiler().compile("stocks#[symbol, exchange]") == HashTag("stocks".f, ArrayLiteral("symbol".f, "exchange".f)))
    }

    it("should compile/decompile: stocks#symbol") {
      assert(LollypopCompiler().compile("stocks#symbol").toSQL == "stocks#symbol")
    }

    it("should compile/decompile: stocks#[symbol, exchange]") {
      assert(LollypopCompiler().compile("stocks#[symbol, exchange]").toSQL == "stocks#[symbol, exchange]")
    }

    it("should create a column table from a durable source") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|drop if exists Stocks
            |create table Stocks (symbol: String(4), exchange: String(6), lastSale: Double)
            |insert into Stocks (symbol, exchange, lastSale)
            |from (
            |    |------------------------------|
            |    | symbol | exchange | lastSale |
            |    |------------------------------|
            |    | AAXX   | NYSE     |    56.12 |
            |    | UPEX   | NYSE     |   116.24 |
            |    | XYZ    | AMEX     |    31.95 |
            |    | ABC    | OTCBB    |    5.887 |
            |    | TRIX   | NYSE     |    77.88 |
            |    | ZZZ    | OTCBB    |   0.0001 |
            |    | ZZY    | OTCBB    |   0.0011 |
            |    | GABY   | NASDAQ   |    13.99 |
            |    |------------------------------|
            |)
            |val stocks = ns('Stocks')
            |stocks#symbol
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX"),
        Map("symbol" -> "UPEX"),
        Map("symbol" -> "XYZ"),
        Map("symbol" -> "ABC"),
        Map("symbol" -> "TRIX"),
        Map("symbol" -> "ZZZ"),
        Map("symbol" -> "ZZY"),
        Map("symbol" -> "GABY")
      ))
    }

    it("should create a column table from a semi-durable source") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|val stocks = Table(symbol: String(4), exchange: String(6), lastSale: Double)
            |insert into @stocks (symbol, exchange, lastSale)
            |from (
            |    |------------------------------|
            |    | symbol | exchange | lastSale |
            |    |------------------------------|
            |    | AAXX   | NYSE     |    56.12 |
            |    | UPEX   | NYSE     |   116.24 |
            |    | XYZ    | AMEX     |    31.95 |
            |    | ABC    | OTCBB    |    5.887 |
            |    | TRIX   | NYSE     |    77.88 |
            |    | ZZZ    | OTCBB    |   0.0001 |
            |    | ZZY    | OTCBB    |   0.0011 |
            |    | GABY   | NASDAQ   |    13.99 |
            |    |------------------------------|
            |)
            |stocks#[symbol, exchange]
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE"), Map("symbol" -> "UPEX", "exchange" -> "NYSE"),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX"), Map("symbol" -> "ABC", "exchange" -> "OTCBB"),
        Map("symbol" -> "TRIX", "exchange" -> "NYSE"), Map("symbol" -> "ZZZ", "exchange" -> "OTCBB"),
        Map("symbol" -> "ZZY", "exchange" -> "OTCBB"), Map("symbol" -> "GABY", "exchange" -> "NASDAQ")
      ))
    }

    it("should create a column table from an ephemeral source") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|declare table stocks(symbol: String(4), exchange: String(6), lastSale: Double)
            |insert into @stocks (symbol, exchange, lastSale)
            |from (
            |    |------------------------------|
            |    | symbol | exchange | lastSale |
            |    |------------------------------|
            |    | AAXX   | NYSE     |    56.12 |
            |    | UPEX   | NYSE     |   116.24 |
            |    | XYZ    | AMEX     |    31.95 |
            |    | ABC    | OTCBB    |    5.887 |
            |    | TRIX   | NYSE     |    77.88 |
            |    | ZZZ    | OTCBB    |   0.0001 |
            |    | ZZY    | OTCBB    |   0.0011 |
            |    | GABY   | NASDAQ   |    13.99 |
            |    |------------------------------|
            |)
            |stocks#[symbol, lastSale]
            |""".stripMargin)
      assert(device.toMapGraph == List(
        Map("symbol" -> "AAXX", "lastSale" -> 56.12), Map("symbol" -> "UPEX", "lastSale" -> 116.24),
        Map("symbol" -> "XYZ", "lastSale" -> 31.95), Map("symbol" -> "ABC", "lastSale" -> 5.887),
        Map("symbol" -> "TRIX", "lastSale" -> 77.88), Map("symbol" -> "ZZZ", "lastSale" -> 1.0E-4),
        Map("symbol" -> "ZZY", "lastSale" -> 0.0011), Map("symbol" -> "GABY", "lastSale" -> 13.99)
      ))
    }

    it("should indicate when an invalid type is specified") {
      Try(LollypopVM.searchSQL(Scope(),
        """|val array = ['A' to 'Z']
           |array#symbol
           |""".stripMargin)) match {
        case Failure(e) => assert(e.getMessage == "Unexpected type returned 'Array('A', 'B', 'C', 'D', 'E', 'F', 'G', ... ' near 'array' on line 2 at 1")
        case Success(value) => fail(s"$value returned, but Exception expected")
      }
    }

  }

}
