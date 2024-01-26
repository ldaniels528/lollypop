package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models._
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import java.io.File
import scala.language.implicitConversions
import scala.util.Properties

/**
 * Changes the current directory.
 * @param expression the optional expression representing the path (e.g. '~/Downloads')
 * @example {{{
 *  cd ~/Downloads
 *  pwd
 * }}}
 */
case class ChDir(expression: Option[Expression]) extends RuntimeExpression {
  import ChDir._

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val prevPath = getCWD
    expression match {
      case Some(expr) =>
        val (sa, ca, path) = expr.pullFile map (_.getPath)
        path match {
          // cd '/some/dir'
          case newPath if newPath.startsWith(File.separator) =>
            (sa.chdir(newPath, prevPath), ca, ())
          // cd './a/path/to/'
          case newPath =>
            val newCurrentPath = new File(prevPath, newPath).getPath
            (sa.chdir(newCurrentPath, prevPath), ca, ())
        }
      // cd
      case None =>
        val newPath = Properties.userHome
        (scope.chdir(newPath, prevPath), IOCost.empty, newPath)
    }
  }

  override def toSQL: String = (keyword :: expression.map(_.toSQL).toList).mkString(" ")

}

object ChDir extends ExpressionParser {
  private val keyword = "cd"
  private val templateCard = s"$keyword ?%g:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Changes the current directory.",
    example =
      """|cd ^/Users/karl/Documents
         |pwd
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Changes the current directory.",
    example =
      """|cd ~/Documents
         |pwd
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Changes the current directory.",
    example =
      """|cd "~/Documents"
         |pwd
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ChDir] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(ChDir(p.expressions.get("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}

