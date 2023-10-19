package com.qwery.runtime.instructions.queryables

import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Queryable
import com.qwery.language.{HelpDoc, QueryableChainParser, SQLCompiler, SQLTemplateParams, TokenStream}

object Having extends QueryableChainParser {
  private val template = "having %c:having"

  override def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val params = SQLTemplateParams(ts, template)
    val having = params.conditions.get("having")
    host match {
      case s: Select if s.having.isEmpty => s.copy(having = having)
      case _ => Select(from = Option(host), having = having)
    }
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "having",
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = template,
    description = "Applies a filter condition to an aggregate query",
    example =
      """|declare table travelers(id UUID, lastName String(12), firstName String(12), destAirportCode String(3))
         |containing (
         ||-------------------------------------------------------------------------------|
         || id                                   | lastName | firstName | destAirportCode |
         ||-------------------------------------------------------------------------------|
         || b938c7e6-76b4-4c0c-a849-8c6f05474270 | JONES    | GARRY     | SNA             |
         || 10b6b0c1-cb90-4708-a57a-8e944bdbdd99 | JONES    | DEBBIE    | SNA             |
         || 10b6b0c1-cb90-4708-a57a-8e944bdb3499 | MILLER   | SALLY MAE | MOB             |
         || 326d3c64-d09d-49ef-972d-cbf815e50c16 | JONES    | TAMERA    | SNA             |
         || c573d368-0f54-4f57-be31-4e6fe0059c23 | JONES    | ERIC      | SNA             |
         || b493e970-2814-4d7c-9003-dcc198b0a539 | ADAMS    | KAREN     | DTW             |
         || 8ab14ae0-a893-430d-bc3e-f4860d5feecb | ADAMS    | MIKE      | DTW             |
         || 8e3229f0-0ac7-45a2-90b2-7fca72e4918e | JONES    | SAMANTHA  | BUR             |
         || 10b6b0c1-cb90-4708-a1b0-8e944bdb34ee | MILLER   | CAROL ANN | MOB             |
         || 324befc4-e584-4b94-a1b0-47faa0cb7b45 | SHARMA   | PANKAJ    | LAX             |
         ||-------------------------------------------------------------------------------|
         |)
         |graph { shape: "bar", title: "Travelers" }
         |select lastName, members: count(*)
         |from @@travelers
         |group by lastName having members > 1
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "having"

}