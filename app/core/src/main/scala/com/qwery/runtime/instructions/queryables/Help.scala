package com.qwery.runtime.instructions.queryables

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, QweryUniverse}
import com.qwery.runtime.Scope
import com.qwery.runtime.datatypes.{StringType, TableType}
import com.qwery.runtime.devices.RecordCollectionZoo.MapToRow
import com.qwery.runtime.devices.RowCollectionZoo._
import com.qwery.runtime.devices.{RowCollection, TableColumn}
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.instructions.expressions.TableExpression
import com.qwery.runtime.instructions.functions.{FunctionCallParserE0Or1, ScalarFunctionCall}
import com.qwery.runtime.instructions.queryables.Help.{commandColumns, gatherHelp}
import com.qwery.util.StringHelper.StringEnrichment
import qwery.io.IOCost

/**
 * Help Database
 * @param name the name (or pattern) of text to search
 * @example {{{
 * select paradigm, category, total: count(*) from (help()) group by paradigm, category order by paradigm
 * }}}
 */
case class Help(name: Option[Expression]) extends ScalarFunctionCall with RuntimeQueryable with TableExpression {
  override def search()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    implicit val ctx: QweryUniverse = scope.getUniverse
    gatherHelp(name_? = name.flatMap(_.asString)) ~> { case (c, r) => (scope, c, r) }
  }

  override def returnType: TableType = TableType(commandColumns)
}

object Help extends FunctionCallParserE0Or1(
  name = "help",
  category = CATEGORY_SYSTEM_TOOLS,
  paradigm = PARADIGM_DECLARATIVE,
  description =
    """|Provides offline manual pages for instructions.
       |Additionally, it's an internal database containing information about every loaded instruction.
       |""".stripMargin,
  examples = List(
    "transpose(help('select'))",
    """|select paradigm, total: count(*)
       |from (help())
       |group by paradigm
       |""".stripMargin,
    """|chart = { shape: "ring", title: "Help By Category" }
       |graph chart from (
       |    select category, total: count(*)
       |    from (help())
       |    group by category
       |)
       |""".stripMargin
  )) {

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

  def gatherHelp(name_? : Option[String])(implicit ctx: QweryUniverse): (IOCost, RowCollection) = {
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

}
