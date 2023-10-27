package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}

/**
 * Where - Filters a result set (Queryable)
 */
object Where extends QueryableChainParser {
  private val template = "where %c:condition"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val where = params.conditions.get("condition")
    host match {
      case s: Select if s.where.isEmpty => s.copy(where = where)
      case _ => Select(from = Option(host), where = where)
    }
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "where",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Filters a result set",
    example =
      """|from (
         ||---------------------------------------------------------|
         || symbol | exchange | lastSale | lastSaleTime             |
         ||---------------------------------------------------------|
         || HWWM   | NASDAQ   | 191.6725 | 2023-08-06T18:33:08.661Z |
         || VLYW   | AMEX     | 197.9962 | 2023-08-06T18:33:08.670Z |
         || VSOM   | NASDAQ   | 166.8542 | 2023-08-06T18:33:08.672Z |
         || FHWS   | NYSE     |  22.5909 | 2023-08-06T18:33:08.673Z |
         || SRGN   | AMEX     | 180.2358 | 2023-08-06T18:33:08.675Z |
         || PTFY   | NYSE     |  19.9265 | 2023-08-06T18:33:08.676Z |
         ||---------------------------------------------------------|
         |) where lastSale < 50.0
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "where"

}