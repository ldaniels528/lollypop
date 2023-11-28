package com.lollypop.runtime.instructions.queryables

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, LollypopUniverse, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.datatypes.{StringType, TableType}
import com.lollypop.runtime.devices.RecordCollectionZoo.MapToRow
import com.lollypop.runtime.devices.RowCollectionZoo._
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.expressions.TableExpression
import com.lollypop.runtime.instructions.queryables.Help.{commandColumns, gatherHelp, keyword}
import com.lollypop.util.StringHelper.StringEnrichment
import lollypop.io.IOCost

/**
 * Help Database
 * @param patternExpr the name (or pattern) of text to search
 * @example {{{
 * select paradigm, category, total: count(*) from (help) group by paradigm, category order by paradigm
 * }}}
 */
case class Help(patternExpr: Option[Expression]) extends RuntimeQueryable with TableExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    implicit val ctx: LollypopUniverse = scope.getUniverse
    patternExpr match {
      case Some(pattern) =>
        val (sa, ca, name) = pattern.pullString
        gatherHelp(name_? = Some(name)) ~> { case (cb, vb) => (sa, ca ++ cb, vb) }
      case None =>
        gatherHelp(name_? = None) ~> { case (cb, vb) => (scope, cb, vb) }
    }
  }

  override def returnType: TableType = TableType(commandColumns)

  override def toSQL: String = (keyword :: patternExpr.map(_.toSQL).toList).mkString(" ")
}

object Help extends QueryableParser {
  private val keyword = "help"
  private val templateCard = s"$keyword ?%g:pattern"

  override def help: List[HelpDoc] = List(
    "help 's(.*)'",
    "help 'select'",
    "transpose(help('select'))",
    """|select category, total: count(*)
       |from (help)
       |group by category
       |order by category
       |""".stripMargin,
    """|chart = { shape: "ring", title: "Help By Category" }
       |graph chart from (
       |    select category, total: count(*)
       |    from (help)
       |    group by category
       |)
       |""".stripMargin,
    """|chart = { shape: "pie3d", title: "Help By Paradigm" }
       |graph chart from (
       |    select paradigm, total: count(*)
       |    from (help)
       |    group by paradigm
       |)
       |""".stripMargin
  ).map { example =>
    HelpDoc(
      name = "help",
      category = CATEGORY_SYSTEM_TOOLS,
      paradigm = PARADIGM_DECLARATIVE,
      description =
        """|Provides offline manual pages for instructions.
           |Additionally, it's an internal database containing information about every loaded instruction.
           |""".stripMargin,
      example = example)
  }

  private val commandColumns = List(
    TableColumn(name = "name", `type` = StringType),
    TableColumn(name = "category", `type` = StringType),
    TableColumn(name = "paradigm", `type` = StringType),
    TableColumn(name = "description", `type` = StringType),
    TableColumn(name = "example", `type` = StringType))

  private def matches(name_? : Option[String], name: String): Boolean = {
    val pattern_? = name_?.map(_.replace("%", ".*"))
    pattern_?.isEmpty || pattern_?.exists(name.matches)
  }

  def gatherHelp(name_? : Option[String])(implicit ctx: LollypopUniverse): (IOCost, RowCollection) = {
    val helpList = ctx.helpDocs.filter(h => matches(name_?, h.name))
    implicit val out: RowCollection = createQueryResultTable(commandColumns, fixedRowCount = helpList.size)
    val cost = out.insert(helpList.map(toMap).map(_.toRow))
    (cost, out)
  }

  private def toMap(help: HelpDoc): Map[String, String] = {
    Map(
      "name" -> help.name.singleLine,
      "category" -> help.category,
      "description" -> help.description.singleLine,
      "example" -> help.example.singleLine,
      "paradigm" -> help.paradigm
    )
  }

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Help] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Help(p.expressions.get("pattern")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
