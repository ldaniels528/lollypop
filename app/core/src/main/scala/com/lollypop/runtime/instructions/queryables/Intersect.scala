package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{BinaryQueryable, Queryable}
import com.lollypop.language.{HelpDoc, QueryableChainParser, SQLCompiler, TokenStream}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a Intersection operation
 * @param a the first [[Queryable queryable resource]]
 * @param b the second [[Queryable queryable resource]]
 */
case class Intersect(a: Queryable, b: Queryable) extends RuntimeQueryable with BinaryQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = a.search(scope)
    val (scopeB, costB, deviceB) = b.search(scopeA)
    (scopeB, costA ++ costB, deviceA intersect deviceB)
  }

  override def toSQL: String = s"${a.toSQL} intersect (${b.toSQL})"

}

object Intersect extends QueryableChainParser {
  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    ts.expect("intersect")
    Intersect(a = host, b = Queryable(ts))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "intersect",
    category = CATEGORY_DATAFRAMES_IO,
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