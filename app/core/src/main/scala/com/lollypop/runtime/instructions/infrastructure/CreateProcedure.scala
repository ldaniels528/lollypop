package com.lollypop.runtime.instructions.infrastructure

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Procedure
import com.lollypop.runtime.DatabaseManagementSystem.createProcedure
import com.lollypop.runtime.{DatabaseObjectRef, Scope}
import lollypop.io.IOCost

import scala.collection.mutable

/**
 * create procedure statement
 * @param ref         the [[DatabaseObjectRef persistent object reference]]
 * @param procedure   the given [[Procedure procedure]]
 * @param ifNotExists if true, the operation will not fail when the entity exists
 * @example {{{
 * create procedure temp.jdbc.getStockQuote(theExchange: String,
 *                                            --> exchange: String,
 *                                            --> total: Double,
 *                                            --> maxPrice: Double,
 *                                            --> minPrice: Double) {
 *    select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
 *    from temp.jdbc.StockQuotes
 *    where exchange is theExchange
 *    group by exchange
 * }
 * }}}
 */
case class CreateProcedure(ref: DatabaseObjectRef, procedure: Procedure, ifNotExists: Boolean)
  extends RuntimeModifiable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, IOCost) = {
    val cost = createProcedure(ref.toNS, procedure, ifNotExists)
    (scope, cost, cost)
  }

  override def toSQL: String = {
    val sb = new mutable.StringBuilder("create procedure ")
    if (ifNotExists) sb.append("if not exists ")
    sb.append(s"${ref.toSQL}${procedure.params.map(_.toSQL).mkString("(", ", ", ")")} := ${procedure.code.toSQL}")
    sb.toString()
  }

  override def toString: String = s"${getClass.getSimpleName}($procedure, $ifNotExists)"

}

object CreateProcedure extends ModifiableParser with IfNotExists {
  private val template: String = "create procedure ?%IFNE:exists %L:name ?%FP:params %C(_|:=|as) %N:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "create procedure",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = template,
    description = "Creates a database procedure",
    example =
      """|namespace "test.examples"
         |stockQuotes =
         | |---------------------------------------------------------|
         | | symbol | exchange | lastSale | lastSaleTime             |
         | |---------------------------------------------------------|
         | | DBGK   | AMEX     |  46.2471 | 2023-08-06T04:50:07.478Z |
         | | GROT   | NASDAQ   |  44.3673 | 2023-08-06T04:50:07.480Z |
         | | SCOF   | NASDAQ   |  60.8058 | 2023-08-06T04:50:07.482Z |
         | | CYCR   | NASDAQ   |  83.9982 | 2023-08-06T04:50:07.483Z |
         | | IIDA   | NASDAQ   | 126.3182 | 2023-08-06T04:50:07.484Z |
         | |---------------------------------------------------------|
         |drop if exists getStockQuote
         |create procedure getStockQuote(theExchange: String,
         |                               --> exchange: String,
         |                               --> total: Double,
         |                               --> maxPrice: Double,
         |                               --> minPrice: Double) :=
         |    select exchange, total: count(*), maxPrice: max(lastSale), minPrice: min(lastSale)
         |    from @@stockQuotes
         |    where exchange is @theExchange
         |    group by exchange
         |
         |call getStockQuote("NASDAQ")
         |""".stripMargin
  ))

  override def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): CreateProcedure = {
    val params = SQLTemplateParams(ts, template)
    CreateProcedure(ref = params.locations("name"),
      Procedure(
        params = params.parameters.getOrElse("params", Nil),
        code = params.instructions("code"),
      ), ifNotExists = params.indicators.get("exists").contains(true))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "create procedure"

}
