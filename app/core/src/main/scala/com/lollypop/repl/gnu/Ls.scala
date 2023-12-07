package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.Expression
import com.lollypop.repl.gnu.Ls.keyword
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.queryables.RuntimeQueryable
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost
import lollypop.lang.OS.generateFileList

/**
 * Returns a dataframe containing a list of files matching any specified criterion.
 * @param pathExpr the optional [[Expression expression]] representing the path (e.g. "~/Downloads/")
 * @example {{{
 *  ls
 *  ls app/examples
 *  ls ~/Downloads
 *  ls _/temp
 *  ls "./app/examples/"
 *  ls "~/Downloads/"
 *  ls "/temp"
 * }}}
 */
case class Ls(pathExpr: Option[Expression]) extends RuntimeQueryable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, RowCollection) = {
    (pathExpr || getCWD.v).pullFile ~>> (generateFileList(_, _.streamFiles))
  }

  override def toSQL: String = (keyword :: pathExpr.map(_.toSQL).toList).mkString(" ")

}

object Ls extends QueryableParser {
  private val keyword = "ls"
  private val templateCard = s"$keyword ?%g:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.",
    example =
      """|ls
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.",
    example =
      """|ls app/examples where not isHidden order by length desc
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.",
    example =
      """|ls "./app/examples/" where name matches ".*[.]csv"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion.\nNOTE: ~ indicates the user's home directory.",
    example =
      """|ls ~ order by length desc limit 5
         |""".stripMargin
  ))

  override def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Ls] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Ls(p.expressions.get("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
