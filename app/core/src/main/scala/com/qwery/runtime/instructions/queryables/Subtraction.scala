package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Represents a Subtraction operation; which returns the subtraction of two queries.
 * @param query0 the first [[Queryable queryable resource]]
 * @param query1 the second [[Queryable queryable resource]]
 */
case class Subtraction(query0: Queryable, query1: Queryable) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = QweryVM.search(scope, query0)
    val (scopeB, costB, deviceB) = QweryVM.search(scopeA, query1)
    (scopeB, costA ++ costB, deviceA subtract deviceB)
  }

  override def toSQL: String = s"${query0.toSQL} subtract ${query1.toSQL}"

}

object Subtraction extends QueryableChainParser {
  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    ts.expect("subtract")
    Subtraction(query0 = host, query1 = Queryable(ts))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "subtract",
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "%q:query0 subtract %q:query1",
    description = "Computes the subtraction of two queries",
    example =
      """|from (
         |    |------------------------------|
         |    | symbol | exchange | lastSale |
         |    |------------------------------|
         |    | AAXX   | NYSE     |    56.12 |
         |    | UPEX   | NYSE     |   116.24 |
         |    | JUNK   | AMEX     |    97.61 |
         |    | XYZ    | AMEX     |    31.95 |
         |    |------------------------------|
         |) subtract (
         |    |------------------------------|
         |    | symbol | exchange | lastSale |
         |    |------------------------------|
         |    | JUNK   | AMEX     |    97.61 |
         |    | ABC    | OTCBB    |    5.887 |
         |    | XYZ    | AMEX     |    31.95 |
         |    |------------------------------|
         |)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "subtract"

}