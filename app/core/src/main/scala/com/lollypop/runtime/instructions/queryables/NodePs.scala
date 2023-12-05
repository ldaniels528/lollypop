package com.lollypop.runtime.instructions.queryables

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.datatypes.{Int32Type, Int64Type, StringType}
import com.lollypop.runtime.devices.RecordCollectionZoo._
import com.lollypop.runtime.devices.RowCollectionZoo.createQueryResultTable
import com.lollypop.runtime.devices.{RowCollection, TableColumn}
import com.lollypop.runtime.instructions.queryables.NodePs.{keyword, listProcesses}
import lollypop.io.IOCost

/**
 * Returns a dataframe containing a list of active Lollypop peers
 * @param expression the optional [[Expression expression]]
 */
case class NodePs(expression: Option[Expression]) extends RuntimeQueryable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    listProcesses()
  }

  override def toSQL: String = (keyword :: expression.map(_.toSQL).toList).mkString(" ")
}

object NodePs extends QueryableParser {
  private val keyword = "nps"
  private val templateCard = s"$keyword ?%g:args"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of active Lollypop peers",
    example =
      """|node = Nodes.start()
         |node.awaitStartup(Duration('1 second'))
         |node.exec("this where kind is 'Table'")
         |nps
         |""".stripMargin
  ))

  def listProcesses()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    val out = createQueryResultTable(
      Seq("host" -> StringType, "port" -> Int32Type, "uptimeInSeconds" -> Int64Type, "lastCommand" -> StringType)
        .map { case (name, kind) => TableColumn(name, kind) })
    scope.getUniverse.nodes.peerNodes foreach { node =>
      out.insert(Map(
        "host" -> node.host,
        "port" -> node.port,
        "uptimeInSeconds" -> (node.uptime / 1000.0),
        "lastCommand" -> node.lastCommand).toRow(out))
    }
    (scope, IOCost.empty, out)
  }

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[NodePs] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(NodePs(p.expressions.get("args")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}