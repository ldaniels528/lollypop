package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.models.@@
import com.lollypop.language.models.Expression.implicits.{LifestyleExpressions, LifestyleExpressionsAny}
import com.lollypop.language.models.Inequality.InequalityExtensions
import com.lollypop.language.models.Operation.RichOperation
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.MacroCall.MacroTemplateTagReplacement
import com.lollypop.runtime.instructions.invocables.SetAnyVariable
import com.lollypop.runtime.{LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec

class MacroCallTest extends AnyFunSpec with VerificationTools {

  describe(classOf[MacroCall].getSimpleName) {

    it("should support creating and invoking an ephemeral macro") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        """|macro 'tickers %e:qty' := {
           |  [1 to qty].map(_ => {
           |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
           |      is_otc = exchange.startsWith("OT")
           |      lastSaleLimit = switch exchange case "OTCBB" then 5.0 case "OTHER_OTC" then 1.0 case _ then 100.0
           |      lastSale = scaleTo(lastSaleLimit * Random.nextDouble(1.0), 4)
           |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
           |      symbol = Random.nextString(['A' to 'Z'], iff(exchange.startsWith("OT"), Random.nextInt(2) + 4, Random.nextInt(4) + 2))
           |      select symbol, exchange, lastSale, lastSaleTime
           |  }).toTable()
           |}
           |
           |tickers 5
           |""".stripMargin
      )
      assert(device.columns.map(_.name).toSet == Set("symbol", "exchange", "lastSale", "lastSaleTime") && device.getLength == 5)
    }

    it("should support creating and invoking a durable macro") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        """|drop if exists `tickers`
           |create macro `tickers` := 'tickers %e:qty' {
           |  [1 to qty].map(_ => {
           |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
           |      is_otc = exchange.startsWith("OT")
           |      lastSaleLimit = switch exchange case "OTCBB" then 5.0 case "OTHER_OTC" then 1.0 case _ then 100.0
           |      lastSale = scaleTo(lastSaleLimit * Random.nextDouble(1.0), 4)
           |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
           |      symbol = Random.nextString(['A' to 'Z'], iff(exchange.startsWith("OT"), Random.nextInt(2) + 4, Random.nextInt(4) + 2))
           |      select symbol, exchange, lastSale, lastSaleTime
           |  }).toTable()
           |}
           |
           |tickers 5
           |""".stripMargin
      )
      assert(results.columns.map(_.name).toSet == Set("symbol", "exchange", "lastSale", "lastSaleTime") && results.getLength == 5)
    }

    it("should support maintain state between macro calls") {
      val (scope, _, _) = LollypopVM.executeSQL(Scope(),
        """|macro 'tickers %e:qty' := {
           |  [1 to qty].map(_ => {
           |      exchange = ['AMEX', 'NASDAQ', 'NYSE', 'OTCBB', 'OTHER_OTC'][Random.nextInt(5)]
           |      is_otc = exchange.startsWith("OT")
           |      lastSaleLimit = switch exchange case "OTCBB" then 5.0 case "OTHER_OTC" then 1.0 case _ then 100.0
           |      lastSale = scaleTo(lastSaleLimit * Random.nextDouble(1.0), 4)
           |      lastSaleTime = DateTime(DateTime() - Duration(1000 * 60 * Random.nextDouble(1.0)))
           |      symbol = Random.nextString(['A' to 'Z'], iff(exchange.startsWith("OT"), Random.nextInt(2) + 4, Random.nextInt(4) + 2))
           |      select symbol, exchange, lastSale, lastSaleTime
           |  }).toTable()
           |}
           |
           |val stocksA = tickers 5
           |val stocksB = tickers 7
           |""".stripMargin
      )
      val stocksA = scope.resolveTableVariable("stocksA")
      val stocksB = scope.resolveTableVariable("stocksB")
      assert(stocksA.columns.map(_.name).toSet == Set("symbol", "exchange", "lastSale", "lastSaleTime") && stocksA.getLength == 5)
      assert(stocksB.columns.map(_.name).toSet == Set("symbol", "exchange", "lastSale", "lastSaleTime") && stocksB.getLength == 7)
    }

  }

  describe(classOf[MacroTemplateTagReplacement].getSimpleName) {

    it("should replace template parameters in: 'calc %e:expr'") {
      val template = "calc %e:expr"
      val result = template.replaceTags(Map("expr" -> ("x".f + "y".f)))
      assert(result == "calc x + y")
    }

    it("should replace template parameters in: 'update %L:name %i:modification ?where +?%c:condition ?limit +?%e:limit'") {
      val template = "update %L:name %i:modification ?where +?%c:condition ?limit +?%e:limit"
      val result = template.replaceTags(Map(
        "name" -> @@("stocks"),
        "modification" -> SetAnyVariable("lastSale".f, 78.11.v),
        "condition" -> ("symbol".f is "AAPL".v),
        "limit" -> 20.v
      ))
      assert(result == """update @stocks set lastSale = 78.11 where symbol is "AAPL" limit 20""")
    }

  }

}
