package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.qwery.language.models.Inequality.InequalityExtensions
import com.qwery.language.models.{@@@, AllFields, Parameter}
import com.qwery.runtime.errors.ResourceNotAutoCloseableException
import com.qwery.runtime.instructions.VerificationTools
import com.qwery.runtime.instructions.functions.{AnonymousFunction, NS}
import com.qwery.runtime.instructions.queryables.Select
import com.qwery.runtime.{QweryCompiler, QweryVM, Scope}
import com.qwery.util.OptionHelper.implicits.risky._
import org.scalatest.funspec.AnyFunSpec

class WithTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: QweryCompiler = QweryCompiler()

  describe(classOf[With].getSimpleName) {

    it("should decompile itself") {
      val model = With(
        resource = NS("temp.instructions.stocks".v),
        code = AnonymousFunction(
          params = List(new Parameter(name = "quotes", `type` = "Any".ct)),
          code = Select(fields = Seq(AllFields), from = @@@("quotes"), where = "lastSale".f < 100.v),
          origin = None))
      assert(model.toSQL ==
        """|with ns("temp.instructions.stocks") (quotes: Any) => select * from @@quotes where lastSale < 100
           |""".stripMargin.split("\n").map(_.trim).mkString(" "))
    }

    it("should open, use then close a resource") {
      val (_, _, results) = QweryVM.searchSQL(Scope(),
        """|namespace 'temp.instructions'
           |drop if exists `stocks` &&
           |create table `stocks` (
           |    symbol: String(8),
           |    exchange: Enum (AMEX, NASDAQ, NYSE, OTCBB, OTHEROTC),
           |    lastSale: Double
           |) containing (
           |    |------------------------------|
           |    | symbol | exchange | lastSale |
           |    |------------------------------|
           |    | AAXX   | NYSE     |    56.12 |
           |    | UPEX   | NYSE     |   116.24 |
           |    | XYZ    | AMEX     |    31.95 |
           |    | JUNK   | AMEX     |    97.61 |
           |    | ABC    | OTCBB    |    5.887 |
           |    |------------------------------|
           |)
           |
           |with ns("temp.instructions.stocks") quotes => select * from @@quotes where lastSale < 100
           |""".stripMargin)
      assert(results.toMapGraph == List(
        Map("symbol" -> "AAXX", "exchange" -> "NYSE", "lastSale" -> 56.12),
        Map("symbol" -> "XYZ", "exchange" -> "AMEX", "lastSale" -> 31.95),
        Map("symbol" -> "JUNK", "exchange" -> "AMEX", "lastSale" -> 97.61),
        Map("symbol" -> "ABC", "exchange" -> "OTCBB", "lastSale" -> 5.887)
      ))
    }

    it("should fail if not AutoCloseable resource") {
      assertThrows[ResourceNotAutoCloseableException] {
        QweryVM.executeSQL(Scope(),
          """|with "Hello" { value => out <=== value }
             |""".stripMargin)
      }
    }

  }

}
