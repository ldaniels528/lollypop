package com.qwery.runtime.instructions.expressions

import com.github.ldaniels528.qwery.StockQuote
import com.qwery.language.models.Expression.implicits._
import com.qwery.language.{QweryUniverse, Template}
import com.qwery.runtime.errors.InterfaceArgumentsNotSupportedError
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class NewTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[New].getSimpleName) {

    it("should create an instance from an interface/trait") {
      val ctx = QweryUniverse(isServerMode = true)
      val (scope, _, _) = QweryVM.executeSQL(ctx.createRootScope(),
        """|import "java.awt.event.MouseListener"
           |import "java.awt.event.MouseEvent"
           |val m = new MouseListener() {
           |    mouseClicked: (e: MouseEvent) => out.println("mouseClicked")
           |    mousePressed: (e: MouseEvent) => out.println("mousePressed")
           |    mouseReleased: (e: MouseEvent) => out.println("mouseReleased")
           |    mouseEntered: (e: MouseEvent) => out.println("mouseEntered")
           |    mouseExited: (e: MouseEvent) => out.println("mouseExited")
           |}
           |m.mouseClicked(null)
           |""".stripMargin)
      assert(scope.getUniverse.system.stdOut.asString().trim == "mouseClicked")
    }

    it("should fail to create an instance with class arguments from an interface/trait") {
      assertThrows[InterfaceArgumentsNotSupportedError] {
        QweryVM.executeSQL(Scope(),
          """|import "java.awt.event.MouseListener"
             |import "java.awt.event.MouseEvent"
             |new MouseListener('Hello World') {
             |    mouseClicked: (e: MouseEvent) => out.println("mouseClicked")
             |    mousePressed: (e: MouseEvent) => out.println("mousePressed")
             |    mouseReleased: (e: MouseEvent) => out.println("mouseReleased")
             |    mouseEntered: (e: MouseEvent) => out.println("mouseEntered")
             |    mouseExited: (e: MouseEvent) => out.println("mouseExited")
             |}
             |""".stripMargin)
      }
    }

    it("should use the template to parse statements") {
      val template = Template(New.templateCard)
      template.tags foreach (t => logger.info(s"|${t.toCode}| ~> $t"))
      val params = template.processWithDebug(
        """|new `java.util.Date`(1631508164812)
           |""".stripMargin)
      logger.info(s"params => ${params.all}")
    }

    it("""should parse "new `java.util.Date`()" """) {
      verify("new `java.util.Date`(1631508164812)", New(typeName = "java.util.Date", args = 1631508164812L.v))
    }

    it("should support decompile select without a from clause") {
      verify(
        """|select symbol: 'GMTQ',
           |       exchange: 'OTCBB',
           |       lastSale: 0.1111,
           |       lastSaleTime: new `java.util.Date`(1631508164812)
           |""".stripMargin)
    }

    it("should support compile select without a from clause") {
      val results = compiler.compile(
        """|select symbol: 'GMTQ',
           |       exchange: 'OTCBB',
           |       lastSale: Double('0.1111'),
           |       lastSaleTime: new `java.util.Date`(Long('1631508164812'))
           |""".stripMargin)
      assert(results == Select(fields = Seq(
        "GMTQ" as "symbol",
        "OTCBB" as "exchange",
        NamedFunctionCall(name = "Double", "0.1111") as "lastSale",
        New(typeName = "java.util.Date", args = NamedFunctionCall("Long", "1631508164812".v)) as "lastSaleTime"
      )))
    }

    it("should instantiate Scala case classes") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|new `com.github.ldaniels528.qwery.StockQuote`(
           |    "ABC",
           |    "OTCBB",
           |    0.0231,
           |    DateTime(1628173110000).getTime()
           |)
           |""".stripMargin)
      assert(result == StockQuote(symbol = "ABC", exchange = "OTCBB", lastSale = 0.0231, lastSaleTime = 1628173110000L))
    }

    it("should instantiate Scala case using an Array with spread operator (...)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|new `com.github.ldaniels528.qwery.StockQuote`(["ABC", "OTCBB", 0.0231, DateTime(1628173110000).getTime()]...)
           |""".stripMargin)
      assert(result == StockQuote(symbol = "ABC", exchange = "OTCBB", lastSale = 0.0231, lastSaleTime = 1628173110000L))
    }

    it("should instantiate Scala case classes using a Dictionary with spread operator (...)") {
      val (_, _, result) = QweryVM.executeSQL(Scope(),
        """|new `com.github.ldaniels528.qwery.StockQuote`({
           |    symbol: "ABC",
           |    exchange: "OTCBB",
           |    lastSale: 0.0231,
           |    lastSaleTime: DateTime(1628173110000).getTime()
           |}...)
           |""".stripMargin)
      assert(result == StockQuote(symbol = "ABC", exchange = "OTCBB", lastSale = 0.0231, lastSaleTime = 1628173110000L))
    }

    it("should instantiate Qwery plastic classes") {
      val (_, _, value) = QweryVM.executeSQL(Scope(),
        """|class ZStock(symbol: String, exchange: String, lastSale: Double, lastSaleTime: DateTime)
           |val stock = new ZStock("AAPL", "NASDAQ", 31.23, new `java.util.Date`())
           |stock.lastSale
           |""".stripMargin)
      assert(value == 31.23)
    }

  }

}