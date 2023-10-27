package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, TokenStream}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Represents a Intersection operation
 * @param query0 the first [[Queryable queryable resource]]
 * @param query1 the second [[Queryable queryable resource]]
 */
case class Intersect(query0: Queryable, query1: Queryable) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = QweryVM.search(scope, query0)
    val (scopeB, costB, deviceB) = QweryVM.search(scopeA, query1)
    (scopeB, costA ++ costB, deviceA intersect deviceB)
  }

  override def toSQL: String = s"${query0.toSQL} intersect (${query1.toSQL})"

}

object Intersect extends QueryableChainParser {
  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    ts.expect("intersect")
    Intersect(query0 = host, query1 = Queryable(ts))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "intersect",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "%q:query0 intersect %q:query1",
    description = "Computes the intersection of two queries",
    example =
      """|from (
         |    |------------------------------|
         |    | symbol | exchange | lastSale |
         |    |------------------------------|
         |    | AAXX   | NYSE     |    56.12 |
         |    | UPEX   | NYSE     |   116.24 |
         |    | XYZ    | AMEX     |    31.95 |
         |    | ABC    | OTCBB    |    5.887 |
         |    |------------------------------|
         |) intersect (
         |    |------------------------------|
         |    | symbol | exchange | lastSale |
         |    |------------------------------|
         |    | JUNK   | AMEX     |    97.61 |
         |    | AAXX   | NYSE     |    56.12 |
         |    | ABC    | OTCBB    |    5.887 |
         |    |------------------------------|
         |)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "intersect"

}