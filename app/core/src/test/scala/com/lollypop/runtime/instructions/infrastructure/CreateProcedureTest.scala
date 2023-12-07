package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language._
import com.lollypop.language.models.{$, AllFields, Procedure}
import com.lollypop.runtime.implicits.risky._
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.instructions.expressions.aggregation.{Count, Max, Min}
import com.lollypop.runtime.instructions.invocables.{Return, ScopedCodeBlock}
import com.lollypop.runtime.instructions.queryables.Select
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler}
import org.scalatest.funspec.AnyFunSpec

class CreateProcedureTest extends AnyFunSpec with VerificationTools {
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[CreateProcedure].getSimpleName) {

    it("should support create procedure") {
      val results = compiler.compile(
        """|create procedure testInserts(industry: String) :=
           |  return (
           |    select Symbol, Name, Sector, Industry, SummaryQuote
           |    from Customers
           |    where Industry is $industry
           |  )
           |""".stripMargin)
      assert(results ==
        CreateProcedure(ref = DatabaseObjectRef("testInserts"),
          Procedure(
            params = List("industry String".c),
            code = Return(Select(
              fields = List("Symbol".f, "Name".f, "Sector".f, "Industry".f, "SummaryQuote".f),
              from = DatabaseObjectRef("Customers"),
              where = "Industry".f is $(name = "industry")
            ))
          ), ifNotExists = false))
    }

    it("should support create procedure with OUT parameters") {
      val results = compiler.compile(
        """|create procedure temp.jdbc.getStockQuote(theExchange: String,
           |                                           --> exchange: String,
           |                                           --> total: Double,
           |                                           --> maxPrice: Double,
           |                                           --> minPrice: Double) := {
           |    select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
           |    from temp.jdbc.StockQuotes
           |    where exchange is $theExchange
           |    group by exchange
           |}
           |""".stripMargin)
      assert(results ==
        CreateProcedure(ref = DatabaseObjectRef("temp.jdbc.getStockQuote"),
          Procedure(
            params = List("theExchange String".c, "exchange: String".po, "total Double".po, "maxPrice Double".po, "minPrice Double".po),
            code = ScopedCodeBlock(Select(
              fields = List("exchange".f, Count(AllFields) as "total", Max("lastSale".f) as "maxPrice", Min("lastSale".f) as "minPrice"),
              from = DatabaseObjectRef("temp.jdbc.StockQuotes"),
              where = "exchange".f is $(name = "theExchange"),
              groupBy = Seq("exchange".f)
            ))
          ), ifNotExists = false))
    }

    it("should support decompiling create procedure") {
      verify(
        """|create procedure testInserts(industry: string) :=
           |  return (
           |    select Symbol, Name, Sector, Industry, SummaryQuote
           |    from Customers
           |    where Industry is $industry
           |  )
           |""".stripMargin)
    }

    it("should support decompiling create procedure with OUT parameters") {
      val model = CreateProcedure(ref = DatabaseObjectRef("temp.jdbc.getStockQuote"),
        Procedure(
          params = List("theExchange String".c, "exchange: String".po, "total Double".po, "maxPrice Double".po, "minPrice Double".po),
          code = ScopedCodeBlock(Select(
            fields = List("exchange".f, Count(AllFields) as "total", Max("lastSale".f) as "maxPrice", Min("lastSale".f) as "minPrice"),
            from = DatabaseObjectRef("temp.jdbc.StockQuotes"),
            where = "exchange".f is "theExchange".f,
            groupBy = Seq("exchange".f)
          ))
        ), ifNotExists = false)

      assert(model.toSQL ==
        """|create procedure temp.jdbc.getStockQuote(theExchange: String, --> exchange: String, --> total: Double, --> maxPrice: Double, --> minPrice: Double) := {
           |  select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale) from temp.jdbc.StockQuotes where exchange is theExchange group by exchange
           |}
           |""".stripMargin.trim)
    }

  }

}
