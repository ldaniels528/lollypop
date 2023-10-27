package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}

/**
 * Order By Queryable
 */
object OrderBy extends QueryableChainParser {
  private val template = "order by %o:orderBy"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val orderBy = params.orderedFields("orderBy")
    host match {
      case s: Select if s.orderBy.isEmpty => s.copy(orderBy = orderBy)
      case _ => Select(from = Option(host), orderBy = orderBy)
    }
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "order by",
    category = CATEGORY_AGG_SORT_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Sorts a result set by a column",
    example =
      """|from (
         | |---------------------------------------------------------|
         | | symbol | exchange | lastSale | lastSaleTime             |
         | |---------------------------------------------------------|
         | | HWWM   | NASDAQ   | 191.6725 | 2023-08-06T18:33:08.661Z |
         | | VLYW   | AMEX     | 197.9962 | 2023-08-06T18:33:08.670Z |
         | | VSOM   | NASDAQ   | 166.8542 | 2023-08-06T18:33:08.672Z |
         | | FHWS   | NYSE     |  22.5909 | 2023-08-06T18:33:08.673Z |
         | | SRGN   | AMEX     | 180.2358 | 2023-08-06T18:33:08.675Z |
         | | PTFY   | NYSE     |  19.9265 | 2023-08-06T18:33:08.676Z |
         | |---------------------------------------------------------|
         |) order by lastSale desc
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "order by"

}