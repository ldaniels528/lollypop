package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.models.Expression.implicits.LifestyleExpressions
import com.lollypop.runtime.instructions.VerificationTools
import com.lollypop.runtime.{DatabaseObjectRef, LollypopCompiler, LollypopVM, Scope}
import org.scalatest.funspec.AnyFunSpec
import org.slf4j.LoggerFactory

class ProcedureCallTest extends AnyFunSpec with VerificationTools {
  private val logger = LoggerFactory.getLogger(getClass)
  implicit val compiler: LollypopCompiler = LollypopCompiler()

  describe(classOf[ProcedureCall].getSimpleName) {

    it("should support compile") {
      val results = compiler.compile("call computeArea(length, width)")
      assert(results == ProcedureCall(ref = DatabaseObjectRef("computeArea"), args = List("length".f, "width".f)))
    }

    it("should support decompile") {
      verify("call computeArea(length, width)")
    }

    it("should support execution via call") {
      val (_, _, device) = LollypopVM.searchSQL(Scope(),
        s"""|declare table results(symbol: String(5), exchange: String(6), lastSale: Double)[5]
            |insert into @@results (symbol, exchange, lastSale)
            |values ('GMTQ', 'OTCBB', 0.1111), ('ABC', 'NYSE', 38.47), ('GE', 'NASDAQ', 57.89)
            |
            |drop if exists temp.jdbc.getStockQuote
            |create procedure temp.jdbc.getStockQuote(theExchange String) := {
            |    stdout <=== 'Selected Exchange: "{{ theExchange }}"'
            |    select exchange, count(*) as total, max(lastSale) as maxPrice, min(lastSale) as minPrice
            |    from @@results
            |    where exchange is theExchange
            |    group by exchange
            |}
            |
            |call temp.jdbc.getStockQuote('NASDAQ')
            |""".stripMargin)
      device.tabulate() foreach logger.info
      assert(device.toMapGraph == List(Map("total" -> 1, "exchange" -> "NASDAQ", "maxPrice" -> 57.89, "minPrice" -> 57.89)))
    }

    it("should support execution via select") {
      val (_, _, results) = LollypopVM.searchSQL(Scope(),
        s"""|declare table results(symbol: String(5), exchange: String(6), lastSale: Double)[5]
            |insert into @@results (symbol, exchange, lastSale)
            |values ('GMTQ', 'OTCBB', 0.1111), ('ABC', 'NYSE', 38.47), ('GE', 'NASDAQ', 57.89)
            |select * from @@results
            |
            |drop if exists temp.jdbc.getStockQuote
            |create procedure temp.jdbc.getStockQuote(theExchange String) := {
            |    stdout <=== 'Selected Exchange: "{{ theExchange }}"'
            |    select exchange, count(*) as total, max(lastSale) as maxPrice, min(lastSale) as minPrice
            |    from @@results
            |    where exchange is theExchange
            |    group by exchange
            |}
            |val getStockQuote = ns('temp.jdbc.getStockQuote')
            |from getStockQuote('NASDAQ')
            |""".stripMargin)
      results.tabulate() foreach logger.info
      assert(results.toMapGraph == List(Map("total" -> 1, "exchange" -> "NASDAQ", "maxPrice" -> 57.89, "minPrice" -> 57.89)))
    }

    it("should support a procedure with OUT parameters") {
      val tableName = getTestTableName
      val procName = s"get$getTestTableName"
      val (_, _, result) = LollypopVM.searchSQL(Scope(),
        s"""|drop if exists $tableName
            |create table $tableName (symbol: String(5), exchange: String(6), lastSale: Double, lastSaleTime: DateTime)
            |containing (
            ||---------------------------------------------------------|
            || symbol | exchange | lastSale | lastSaleTime             |
            ||---------------------------------------------------------|
            || BXXG   | NASDAQ   |   147.63 | 2020-08-01T21:33:11.000Z |
            || KFFQ   | NYSE     |    22.92 | 2020-08-01T21:33:11.000Z |
            || GTKK   | NASDAQ   |   240.14 | 2020-08-07T21:33:11.000Z |
            || KNOW   | OTCBB    |   357.21 | 2020-08-19T21:33:11.000Z |
            ||---------------------------------------------------------|
            |)
            |
            |drop if exists $procName
            |create procedure $procName(theExchange: String,
            |                           --> exchange: String,
            |                           --> total: Double,
            |                           --> maxPrice: Double,
            |                           --> minPrice: Double) := {
            |    select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
            |    from $tableName
            |    where exchange is theExchange
            |    group by exchange
            |}
            |
            |call $procName('NASDAQ')
            |""".stripMargin)
      assert(result.toMapGraph == List(Map("exchange" -> "NASDAQ", "total" -> 2, "maxPrice" -> 240.14, "minPrice" -> 147.63)))
    }

  }

}
