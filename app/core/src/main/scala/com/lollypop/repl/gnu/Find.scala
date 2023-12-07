package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, QueryableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Find.keyword
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost
import lollypop.lang.OS.generateFileList

/**
 * Returns a dataframe containing a recursive list of files matching any specified criterion.
 * @param pathExpr the [[Expression expression]] representing the path (e.g. "./app/examples/")
 * @example {{{
 *  find ~ limit 5
 *  find app/examples where name matches "(.*)[.]csv"
 *  find "./app/examples/" order by lastModified desc
 * }}}
 */
case class Find(pathExpr: Expression) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    pathExpr.pullFile ~>> (generateFileList(_, _.streamFilesRecursively))
  }

  override def toSQL: String = Seq(keyword, pathExpr.toSQL).mkString(" ")

}

object Find extends QueryableParser {
  private val keyword = "find"
  private val templateCard = s"$keyword ?%e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a recursive list of files matching any specified criterion.",
    example =
      """|find "./app/examples/" where name matches "(.*)[.]csv"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.",
    example =
      """|find app/examples where name matches "(.*)[.]csv" order by lastModified desc
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.\nNOTE: ~ indicates the user's home directory.",
    example =
      """|find docs limit 5
         |""".stripMargin
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Find] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Find(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}

