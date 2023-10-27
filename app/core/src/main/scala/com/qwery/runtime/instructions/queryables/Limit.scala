package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Expression, Queryable}
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

case class Limit(source: Queryable, limit: Option[Expression] = None) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope1, cost1, in) = QweryVM.search(scope, source)
    val out = createQueryResultTable(in.columns)
    val cost2 = in.iterateWhere(limit = limit)(_.isActive) { case (_, row) => out.insert(row) }(scope1)
    (scope1, cost1 ++ cost2, out)
  }

  override def toSQL: String = (source.toSQL :: limit.map(n => s"limit ${n.toSQL}").toList).mkString(" ")
}

object Limit extends QueryableChainParser {
  private val template = "limit %e:limit"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val limit = params.expressions.get("limit")
    host match {
      case s: Select if s.limit.isEmpty => s.copy(limit = limit)
      case _ => Limit(host, limit)
    }
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "limit",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Limits the maximum number of rows returned by a query",
    example =
      """|from (
         |    |-------------------------------------------------------------------------|
         |    | ticker | market | lastSale | roundedLastSale | lastSaleTime             |
         |    |-------------------------------------------------------------------------|
         |    | NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
         |    | AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
         |    | WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
         |    | ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
         |    | NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
         |    |-------------------------------------------------------------------------|
         |) limit 3
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "limit"

}