package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{BinaryQueryable, Queryable}
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Represents a union operation; which combines two queries.
 * @param a the first [[Queryable queryable resource]]
 * @param b the second [[Queryable queryable resource]]
 */
case class Union(a: Queryable, b: Queryable) extends RuntimeQueryable with BinaryQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scopeA, costA, deviceA) = a.search(scope)
    val (scopeB, costB, deviceB) = b.search(scopeA)
    (scopeB, costA ++ costB, deviceA union deviceB)
  }

  override def toSQL: String = s"${a.toSQL} union ${b.toSQL}"

}

object Union extends QueryableChainParser {
  private val template = "union ?%C(mode|all|distinct) %Q:query"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val isDistinct = params.atoms.is("mode", _.name == "distinct")
    if (isDistinct) UnionDistinct(a = host, b = params.instructions("query").asQueryable)
    else Union(a = host, b = params.instructions("query").asQueryable)
  }

  override def help: List[HelpDoc] = {
    List(HelpDoc(
      name = "union",
      category = CATEGORY_DATAFRAMES_IO,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "%q:query0 union ?%C(mode|all|distinct) %q:query1",
      featureTitle = Some("Matter of taste"),
      description = "The solution to a problem can be achieved many different ways...",
      example =
        """|import 'java.lang.Runtime'
           |rt = Runtime.getRuntime()
           |
           |chart = { shape: "pie3d", title: "Memory Usage" }
           |graph chart from {
           |    // (1) the following declarative expression ...
           |    [{ name: 'totalMemory', value: rt.totalMemory() },
           |     { name: 'freeMemory', value: rt.freeMemory() }].toTable()
           |
           |    // (2) and the following SQL statement ...
           |    select name: 'totalMemory', value: rt.totalMemory()
           |      union
           |    select name: 'freeMemory', value: rt.freeMemory()
           |
           |    // (3) and the following multi-paradigm statement are all equivalent.
           |    transpose(select totalMemory: rt.totalMemory(), freeMemory: rt.freeMemory())
           |}
           |""".stripMargin
    ), HelpDoc(
      name = "union",
      category = CATEGORY_DATAFRAMES_IO,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "%q:query0 union ?%C(mode|all|distinct) %q:query1",
      description = "Combines two (or more) result sets (vertically)",
      example =
        """|from (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | AAXX   | NYSE     |    56.12 |
           |  | UPEX   | NYSE     |   116.24 |
           |  | XYZ    | AMEX     |    31.95 |
           |  |------------------------------|
           |) union (
           |  |------------------------------|
           |  | symbol | exchange | lastSale |
           |  |------------------------------|
           |  | JUNK   | AMEX     |    97.61 |
           |  | ABC    | OTC BB   |    5.887 |
           |  |------------------------------|
           |)
           |""".stripMargin
    ), HelpDoc(
      name = "union distinct",
      category = CATEGORY_DATAFRAMES_IO,
      paradigm = PARADIGM_DECLARATIVE,
      syntax = "%q:query0 union distinct distinct %q:query1",
      description = "Combines two (or more) result sets (vertically) retaining only distinct rows",
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
           |) union distinct (
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
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "union"

}