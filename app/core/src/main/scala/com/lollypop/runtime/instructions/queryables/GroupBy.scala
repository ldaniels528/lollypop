package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_AGG_SORT_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Queryable
import com.lollypop.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}

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
    category = CATEGORY_AGG_SORT_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Aggregates a result set by a column",
    example =
      s"""|select kind, total: count(*)
          |from (this.toTable())
          |group by kind
          |""".stripMargin
  ), HelpDoc(
    name = "group by",
    category = CATEGORY_AGG_SORT_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Aggregates a result set by a column",
    example =
      s"""|chart = { shape: "bar", title: "Types in Session" }
          |graph chart from (
          |    select kind, total: count(*)
          |    from (this.toTable())
          |    group by kind
          |)
          |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "group by"

}