package com.qwery.runtime.instructions.jvm

import com.qwery.language.models.Expression.implicits.LifestyleExpressions
import com.qwery.language.models.{Column, Parameter}
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DeclareClassTest extends AnyFunSpec {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[DeclareClass].getSimpleName) {

    it("should compile SQL into a model") {
      val model = QweryCompiler().compile(
        """|class Stock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |""".stripMargin)
      assert(model == DeclareClass(className = "Stock", fields = List(
        Column(name = "symbol", `type` = "String".ct),
        Column(name = "exchange", `type` = "String".ct),
        Column(name = "lastSale", `type` = "Double".ct),
        Column(name = "lastSaleTime", `type` = "DateTime".ct)
      )))
    }

    it("should decompile a model into SQL") {
      val model = DeclareClass(className = "Stock", fields = List(
        Parameter(name = "symbol", `type` = "String".ct),
        Parameter(name = "exchange", `type` = "String".ct),
        Parameter(name = "lastSale", `type` = "Double".ct),
        Parameter(name = "lastSaleTime", `type` = "DateTime".ct)
      ))
      assert(model.toSQL == "class Stock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)")
    }

    it("should declare a new class") {
     val (scope, _, _) = QweryVM.executeSQL(Scope(),
        """|class Stock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |""".stripMargin)
      assert(scope.resolve("Stock") contains DeclareClass(className = "Stock", fields = List(
        Column(name = "symbol", `type` = "String".ct),
        Column(name = "exchange", `type` = "String".ct),
        Column(name = "lastSale", `type` = "Double".ct),
        Column(name = "lastSaleTime", `type` = "DateTime".ct)
      )))
    }

    it("should declare a new class and create an instance of it") {
      val (_, _, stock) = QweryVM.executeSQL(Scope(),
        """|class YStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |new YStock("AAPL", "NASDAQ", 31.23, DateTime("2021-08-05T14:18:30.000Z"))
           |""".stripMargin)
      assert(stock.toString == """YStock("AAPL", "NASDAQ", 31.23, "2021-08-05T14:18:30.000Z")""")
    }

    it("should create plastic instances") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|class ZStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |val stock = new ZStock("AAPL", "NASDAQ", 31.23, new `java.util.Date`())
           |stock.symbol
           |""".stripMargin)
      assert(value == "AAPL")
    }

  }

}