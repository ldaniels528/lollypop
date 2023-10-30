package com.lollypop.runtime.plastics

import com.lollypop.language.LollypopUniverse
import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.instructions.jvm.DeclareClass
import com.lollypop.runtime.plastics.Plastic.implicits.ProductToMap
import com.lollypop.runtime.{DynamicClassLoader, LollypopVM, Scope}
import com.lollypop.util.LogUtil
import org.scalatest.funspec.AnyFunSpec

class PlasticProductTest extends AnyFunSpec {
  implicit val ctx: LollypopUniverse = LollypopUniverse()
  implicit val scope: Scope = ctx.createRootScope()
  implicit val cl: DynamicClassLoader = ctx.classLoader

  sealed trait BigMartSKU extends PlasticProduct {

    def a: Int

    def b: Int

    def c: Int

  }

  describe(classOf[BigMartSKU].getSimpleName) {

    val inst: BigMartSKU = PlasticProduct[BigMartSKU](
      declaredClass = DeclareClass(className = "BigMartSKU", fields = List("a Int".c, "b Int".c, "c Int".c)),
      fieldNames = Seq("a", "b", "c"),
      fieldValues = Seq(1, 2, 3)
    )

    it("should produce the appropriate toString message for a proxied Product") {
      assert(inst.toString == "BigMartSKU(1, 2, 3)")
    }

    it("should convert a native Product into a Map") {
      case class Fox(a: Int = 1, b: Int = 5, c: Int = 13)
      assert(Fox().toMap == Map("a" -> 1, "b" -> 5, "c" -> 13))
    }

    it("should convert a proxied Product into a Map") {
      assert(inst.toMap == Map("a" -> 1, "b" -> 2, "c" -> 3))
    }

    it("should retrieve the arity of a proxied Product") {
      assert(inst.productArity == 3)
    }

    it("should perform membersOf() on a proxied Product") {
      val (_, _, rc) = LollypopVM.searchSQL(scope.withVariable("x", inst),
        """|membersOf(x)
           |""".stripMargin)
      rc.tabulate().foreach(LogUtil(this).info)
    }

  }

  describe(classOf[PlasticProduct].getSimpleName) {

    it("should evaluate: stock.symbol") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("AAPL", "NASDAQ", 345.67)
           |stock.symbol
           |""".stripMargin)
      assert(rv == "AAPL")
    }

    it("should evaluate: stock.?symbol()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("AAPL", "NASDAQ", 345.67)
           |stock.?symbol()
           |""".stripMargin)
      assert(rv == false)
    }

    it("should evaluate: (new Stock('QNX', 'OTCBB', 3.4567)).toString()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("QNX", "OTCBB", 3.4567)
           |stock.toString()
           |""".stripMargin)
      assert(rv == "Stock(\"QNX\", \"OTCBB\", 3.4567)")
    }

    it("should evaluate: (new Stock('QNX', 'OTCBB', null)).toString()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("QNX", "OTCBB", null)
           |stock.toString()
           |""".stripMargin)
      assert(rv == "Stock(\"QNX\", \"OTCBB\", null)")
    }

    it("should evaluate: stock.?getClass()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("AAPL", "NASDAQ", 345.67)
           |stock.?getClass()
           |""".stripMargin)
      assert(rv == true)
    }

    it("should evaluate: stock.productArity()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("AAPL", "NASDAQ", 345.67)
           |stock.productArity()
           |""".stripMargin)
      assert(rv == 3)
    }

    it("should evaluate: stock.productElementNames()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("AAPL", "NASDAQ", 345.67)
           |stock.productElementNames().toList()
           |""".stripMargin)
      assert(rv == List("symbol", "exchange", "lastSale"))
    }

    it("should evaluate: stock.productIterator()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("XYZ", "NYSE", null)
           |stock.productIterator().toList()
           |""".stripMargin)
      assert(rv == List("XYZ", "NYSE", null))
    }

    it("should evaluate: stock.productToMap()") {
      val (_, _, rv) = LollypopVM.executeSQL(scope,
        """|class Stock(symbol: String, exchange: String, lastSale: Double)
           |stock = new Stock("ABC", null, 0.6567)
           |stock.productToMap()
           |""".stripMargin)
      assert(rv == Map("symbol" -> "ABC", "exchange" -> null, "lastSale" -> 0.6567))
    }

  }

}
