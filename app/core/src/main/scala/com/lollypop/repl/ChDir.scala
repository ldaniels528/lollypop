package com.lollypop.repl

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.io.File

/**
 * Changes the current directory (in REPL only)
 * @param expression the optional expression representing the path (e.g. './lollypop_db')
 * @example {{{
 *  lollypopComponents('com.lollypop.repl.ChDir$')
 *  cd './lollypop_db'
 *  cd
 * }}}
 */
case class ChDir(expression: Option[Expression]) extends RuntimeExpression {
  import com.lollypop.repl.ChDir.{_CWD_, _PWD_, getCWD, getOldCWD, keyword}

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val prevPath = getCWD

    def enrich(sa: Scope, cwd: String, pwd: String): Scope = {
      sa.withVariable(_CWD_, cwd).withVariable(_PWD_, pwd)
    }

    expression match {
      case Some(expr) =>
        val (sa, ca, rawPath) = expr.pullString
        rawPath match {
          // cd -
          case "-" =>
            val curPath = getOldCWD
            (enrich(sa, curPath, prevPath), ca, curPath)
          // cd '.'
          case "." => (sa, ca, prevPath)
          // cd '..'
          case ".." =>
            val newPath = new File(prevPath).getParent
            (enrich(sa, newPath, prevPath), ca, newPath)
          // cd '/some/dir'
          case s if s.startsWith(File.separator) =>
            (enrich(sa, s, prevPath), ca, ())
          // cd './a/path/to/'
          case s =>
            val newPath = new File(prevPath, s).getPath
            (enrich(sa, newPath, prevPath), ca, ())
        }
      // cd
      case None => (scope, IOCost.empty, prevPath)
    }
  }

  override def toSQL: String = (keyword :: expression.map(_.toSQL).toList).mkString(" ")

}

object ChDir extends ExpressionParser {
  private val _CWD_ = "__current_working_directory__"
  private val _PWD_ = "__previous_working_directory__"
  private val keyword = "cd"
  private val templateCard = s"$keyword ?%e:path"

  def getCWD(implicit scope: Scope): String = getOSPath(_CWD_)

  def getOldCWD(implicit scope: Scope): String = getOSPath(_PWD_)

  def getOSPath(name: String)(implicit scope: Scope): String = {
    scope.resolveAs[String](name) || new File(".").getCanonicalPath
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Changes the current directory (in REPL only)",
    example =
      """|lollypopComponents('com.lollypop.repl.ChDir$')
         |cd './lollypop_db'
         |cd
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

