package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.{Instruction, Queryable}
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.devices.RowCollection
import com.qwery.runtime.{DatabaseObjectRef, QweryVM, Scope}
import qwery.io.IOCost

/**
 * Into - copies rows from a source to a target
 * @param source the queryable [[Instruction source]]
 * @param target the appendable [[DatabaseObjectRef target]]
 * @example {{{ from @@stocks where lastSale < 1.0 into @@pennyStocks }}}
 */
case class Into(source: Instruction, target: DatabaseObjectRef) extends RuntimeQueryable {

  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val (scope1, cost0, in) = QweryVM.search(scope, source)
    val out = scope1.getRowCollection(target)
    val cost1 = in.iterateWhere()(_.isActive) { case (_, row) => out.insert(row) }
    (scope1, cost0 ++ cost1, out)
  }

  override def toSQL: String = List(source.toSQL, "into", target.toSQL).mkString(" ")

}

object Into extends QueryableChainParser {
  private val template = "into %L:target"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    Into(host, target = params.locations("target"))
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "into",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Inserts a result set into a table",
    example =
      """|pennyStocks = Table(symbol: String(10), exchange: String(10), lastSale: Double, lastSaleTime: DateTime)
         |from (
         ||----------------------------------------------------------|
         || exchange  | symbol | lastSale | lastSaleTime             |
         ||----------------------------------------------------------|
         || OTHER_OTC | YVWY   |   0.5407 | 2023-09-21T04:47:37.370Z |
         || OTHER_OTC | EPOFJ  |   0.8329 | 2023-09-21T04:47:27.720Z |
         || OTHER_OTC | QEQA   |   0.7419 | 2023-09-21T04:48:07.901Z |
         || OTHER_OTC | SFWCS  |   0.9577 | 2023-09-21T04:47:54.694Z |
         || OTHER_OTC | VBJHF  |   0.8121 | 2023-09-21T04:47:56.769Z |
         || OTHER_OTC | SDLMF  |   0.2186 | 2023-09-21T04:48:18.913Z |
         || OTHER_OTC | JXDZ   |   0.0157 | 2023-09-21T04:48:08.459Z |
         || OTCBB     | ZMNF   |   0.5647 | 2023-09-21T04:47:23.112Z |
         || OTCBB     | VVAH   |   0.5786 | 2023-09-21T04:47:40.420Z |
         || OTCBB     | HSCKG  |   0.2719 | 2023-09-21T04:47:43.268Z |
         || OTCBB     | SHDF   |   0.0161 | 2023-09-21T04:57:07.529Z |
         || OTCBB     | QJGVO  |   0.0026 | 2023-09-21T04:57:39.230Z |
         || OTHER_OTC | PMBFY  |   0.0139 | 2023-09-21T04:57:46.146Z |
         || OTCBB     | CAVY   |   0.0047 | 2023-09-21T04:57:43.503Z |
         ||----------------------------------------------------------|
         |) where lastSale <= 0.02 into @@pennyStocks
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "into"

}