package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.{$, @@}
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.devices._
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class ElementAtTest extends AnyFunSpec {

  describe(classOf[ElementAt].getSimpleName) {
    
    it("should decompile models: [99, 100, 101][1]") {
      val model = ElementAt(ArrayLiteral(99.v, 100.v, 101.v), 1.v)
      assert(model.toSQL == "[99, 100, 101][1]")
    }

    it("should decompile models: 'Hello World'[4]") {
      val model = ElementAt("Hello World".v, 4.v)
      assert(model.toSQL == "\"Hello World\"[4]")
    }

    it("should decompile models: items[18]") {
      val model = ElementAt("items".f, 18.v)
      assert(model.toSQL == "items[18]")
    }

    it("should decompile models: @stocks[2500]") {
      val model = ElementAt(@@("stocks"), 2500.v)
      assert(model.toSQL == "@stocks[2500]")
    }

    it("should detect the return type: [99, 100, 101][1]") {
      val model = ElementAt(ArrayLiteral(99.v, 100.v, 101.v), 1.v)
      assert(model.returnType == Int32Type)
    }

    it("should detect the return type: 'Hello World'[4]") {
      val model = ElementAt("Hello World".v, 4.v)
      assert(model.returnType == CharType)
    }

    it("should detect the return type: items[18]") {
      val model = ElementAt("items".f, 18.v)
      assert(model.returnType == AnyType)
    }

    it("should detect the return type: @stocks[2500]") {
      val model = ElementAt(@@("stocks"), 2500.v)
      assert(model.returnType == AnyType)
    }

    it("should evaluate: [99, 100, 101][1]") {
      val model = ElementAt(ArrayLiteral(99.v, 100.v, 101.v), 1.v)
      val value = model.execute()(Scope())._3
      assert(value == 100)
    }

    it("should evaluate: 'Hello World'[4]") {
      val model = ElementAt("Hello World".v, 4.v)
      val value = model.execute()(Scope())._3
      assert(value == 'o')
    }

    it("should evaluate: @stocks[2]") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|declare table stocks(symbol: String(4), exchange: String(6), lastSale: Float)
           |stocks.push({ symbol: 'ABC', exchange: 'OTCBB', lastSale: 37.89 })
           |stocks.push({ symbol: 'T', exchange: 'NYSE', lastSale: 22.77 })
           |stocks.push({ symbol: 'AAPL', exchange: 'NASDAQ', lastSale: 149.76 })
           |""".stripMargin)
      val model = ElementAt($("stocks"), 2.v)
      val value = model.execute()(scope)._3
      assert(value ==
        Row(id = 2, metadata = RowMetadata(),
          columns = List(
            TableColumn(name = "symbol", `type` = StringType(4)),
            TableColumn(name = "exchange", `type` = StringType(6)),
            TableColumn(name = "lastSale", `type` = Float32Type)),
          fields = List(
            Field(name = "symbol", metadata = FieldMetadata(), value = Some("AAPL")),
            Field(name = "exchange", metadata = FieldMetadata(), value = Some("NASDAQ")),
            Field(name = "lastSale", metadata = FieldMetadata(), value = Some(149.76f)))
        ))
    }

    it("should retrieve a row from a table") {
      val (_, _, row) = LollypopVM.executeSQL(Scope(),
        """|(
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | AAXX   | NYSE     |    56.12 |
           |  | UPEX   | NYSE     |   116.24 |
           |  | XYZ    | AMEX     |    31.95 |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)[3]
           |""".stripMargin)
      val mapping = Option(row).collect { case r: Row => r.toMap }.orNull
      assert(mapping == Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61))
    }

    it("should retrieve a row from a table variable") {
      val (_, _, row) = LollypopVM.executeSQL(Scope(),
        """|val stocks = (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | AAXX   | NYSE     |    56.12 |
           |  | UPEX   | NYSE     |   116.24 |
           |  | XYZ    | AMEX     |    31.95 |
           |  | JUNK   | AMEX     |    97.61 |
           |  |------------------------------|
           |)
           |stocks[3]
           |""".stripMargin)
      val mapping = Option(row).collect { case r: Row => r.toMap }.orNull
      assert(mapping == Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61))
    }

  }

}
