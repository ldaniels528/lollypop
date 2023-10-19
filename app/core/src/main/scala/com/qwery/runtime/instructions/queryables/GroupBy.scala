package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}

object GroupBy extends QueryableChainParser {
  private val templateCard = "group by %F:groupBy"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, templateCard)
    val groupBy = params.fieldLists.getOrElse("groupBy", Nil)
    host match {
      case s: Select if s.groupBy.isEmpty => s.copy(groupBy = groupBy)
      case _ => Select(from = Option(host), groupBy = groupBy)
    }
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "group by",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Aggregates a result set by a column",
    example =
      s"""|chart = { shape: "ring", title: "Types in Session" }
         |graph chart from (
         |    select kind, total: count(*)
         |    from (this.toTable())
         |    group by kind
         |)
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "group by"

}