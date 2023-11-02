package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_DATAFRAMES_IO, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Queryable
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, TokenStream}
import com.lollypop.runtime.devices.{PaginationSupport, RowCollection}
import com.lollypop.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Pagination - setups a paginated queryable
 * @param queryable the [[Queryable queryable]] to paginate
 */
case class Pagination(queryable: Queryable) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope0, cost0, rc0) = LollypopVM.search(scope, queryable)
    (scope0, cost0, PaginationSupport(rc0))
  }

  override def toSQL: String = s"pagination ${queryable.wrapSQL}"

}

/**
 * Pagination Parser
 */
object Pagination extends QueryableParser {
  override def help: List[HelpDoc] = List(HelpDoc(
    name = "pagination",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "pagination(query)",
    description = "Setups a pagination query",
    example =
      """|stocks =
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
         || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
         || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
         || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
         || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
         || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
         || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
         || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
         || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
         || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
         || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
         || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
         || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
         || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
         || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
         ||----------------------------------------------------------|
         |stocksP = pagination(select * from stocks)
         |stocksP.first(5)
         |""".stripMargin
  ), HelpDoc(
    name = "pagination",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "pagination(query)",
    description = "Setups a pagination query",
    example =
      """|stocks =
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
         || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
         || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
         || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
         || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
         || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
         || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
         || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
         || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
         || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
         || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
         || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
         || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
         || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
         || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
         ||----------------------------------------------------------|
         |stocksP = pagination(select * from stocks)
         |stocksP.last(5)
         |""".stripMargin
  ), HelpDoc(
    name = "pagination",
    category = CATEGORY_DATAFRAMES_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "pagination(query)",
    description = "Setups a pagination query",
    example =
      """|stocks =
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTCBB     | RPYM   |   0.4932 | 2023-10-02T01:57:31.086Z |
         || OTCBB     | EGBQ   |   0.6747 | 2023-10-02T01:57:09.991Z |
         || OTHER_OTC | PEMCQ  |   0.6176 | 2023-10-02T01:57:23.684Z |
         || NASDAQ    | IPHBY  | 113.9129 | 2023-10-02T01:57:01.837Z |
         || NASDAQ    | HLOQW  | 159.1307 | 2023-10-02T01:57:50.139Z |
         || NYSE      | WQN    | 177.4067 | 2023-10-02T01:57:17.371Z |
         || NASDAQ    | JONV   | 139.6465 | 2023-10-02T01:57:55.758Z |
         || NASDAQ    | KKLPE  | 135.2768 | 2023-10-02T01:57:07.520Z |
         || AMEX      | KHGRO  | 163.3631 | 2023-10-02T01:57:21.286Z |
         || NASDAQ    | GSCF   |  75.8721 | 2023-10-02T01:57:21.640Z |
         || NASDAQ    | ZEP    |   91.009 | 2023-10-02T01:57:03.740Z |
         || OTHER_OTC | KMUEH  |   0.2605 | 2023-10-02T01:57:03.702Z |
         || OTCBB     | WLXIM  |   0.6886 | 2023-10-02T01:57:45.739Z |
         || NASDAQ    | OVTS   | 153.5991 | 2023-10-02T01:57:23.061Z |
         || OTCBB     | YGIVQ  |   0.8364 | 2023-10-02T01:57:38.882Z |
         ||----------------------------------------------------------|
         |stocksP = pagination(select * from stocks)
         |stocksP.first(5)
         |stocksP.next(5)
         |""".stripMargin
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Pagination] = {
    if (ts.nextIf("pagination")) {
      compiler.nextExpression(ts) match {
        case Some(queryable: Queryable) => Some(Pagination(queryable))
        case Some(expression) => Some(Pagination(expression.asQueryable))
        case None => ts.dieExpectedQueryable()
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "pagination"

}