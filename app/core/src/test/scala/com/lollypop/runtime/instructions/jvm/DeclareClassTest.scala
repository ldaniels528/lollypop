package com.lollypop.runtime.instructions.jvm

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.language.models.{Column, Parameter}
import com.lollypop.runtime.{LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class DeclareClassTest extends AnyFunSpec {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[DeclareClass].getSimpleName) {

    it("should compile SQL into a model") {
      val model = LollypopCompiler().compile(
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
     val (scope, _, _) = LollypopVM.executeSQL(Scope(),
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
      val (_, _, stock) = LollypopVM.executeSQL(Scope(),
        """|class YStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |new YStock("AAPL", "NASDAQ", 31.23, DateTime("2021-08-05T14:18:30.000Z"))
           |""".stripMargin)
      assert(stock.toString == """YStock("AAPL", "NASDAQ", 31.23, "2021-08-05T14:18:30.000Z")""")
    }

    it("should create plastic instances") {
      val (_, _, value) = LollypopVM.executeSQL(Scope(),
        """|class ZStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |val stock = new ZStock("AAPL", "NASDAQ", 31.23, new `java.util.Date`())
           |stock.symbol
           |""".stripMargin)
      assert(value == "AAPL")
    }

  }

}