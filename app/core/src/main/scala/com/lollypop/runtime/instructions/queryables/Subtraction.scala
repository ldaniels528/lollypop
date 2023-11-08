package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Queryable
import com.lollypop.language.{HelpDoc, QueryableChainParser, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.devices.RowCollection
import lollypop.io.IOCost

/**
 * Represents a Subtraction operation; which returns the subtraction of two queries.
 * @param query0 the first [[Queryable queryable resource]]
 * @param query1 the second [[Queryable queryable resource]]
 */
case class Subtraction(query0: Queryable, query1: Queryable) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = query0.search(scope)
    val (scopeB, costB, deviceB) = query1.search(scopeA)
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
    category = CATEGORY_DATAFRAMES_IO,
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