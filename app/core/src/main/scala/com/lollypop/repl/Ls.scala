package com.lollypop.repl

import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.models.Expression.implicits.LifestyleExpressionsAny
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.ChDir.getCWD
import com.lollypop.repl.Ls.keyword
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.devices.RowCollection
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.operators.{NEG, Tilde}
import lollypop.io.IOCost
import lollypop.lang.OS.generateFileList

import java.io.File

/**
 * Returns a dataframe containing a list of files matching any specified criterion (in REPL only)
 * @param pathExpr the optional expression representing the path (e.g. './lollypop_db')
 * @example {{{
 *  lollypopComponents("com.lollypop.repl.Ls$")
 *  ls
 *  ls ~(".")
 *  ls -("./app/examples/")
 *  ls "./app/examples/"
 * }}}
 */
case class Ls(pathExpr: Option[Expression]) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    import scala.util.Properties.userHome

    def listFiles(path: String, recursive: Boolean): RowCollection = {
      generateFileList(new File(path), if (recursive) _.streamFilesRecursively else _.streamFiles)
    }

    def search(path: Expression, recursive: Boolean): (Scope, IOCost, RowCollection) = {
      path.pullString ~>> { x => listFiles(x, recursive) }
    }

    def recurse(expression: Expression): (Scope, IOCost, RowCollection) = {
      expression match {
        case Tilde(path) =>
          path.pullString ~>> { x => listFiles(new File(userHome, x).getPath, recursive = false) }
        case NEG(path) => search(path, recursive = true)
        case path => search(path, recursive = false)
      }
    }

    pathExpr match {
      case Some(expr) => recurse(expr)
      case None => search(getCWD.v, recursive = false)
    }
  }

  override def toSQL: String = (keyword :: pathExpr.map(_.toSQL).toList).mkString(" ")

}

object Ls extends ExpressionParser {
  private[repl] val templateCard = "ls ?%e:path"
  private val keyword = "ls"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion (in REPL only)",
    example =
      """|lollypopComponents("com.lollypop.repl.Ls$")
         |ls
         |""".stripMargin
  ),HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion (in REPL only)",
    example =
      """|lollypopComponents("com.lollypop.repl.Ls$")
         |ls "./app/examples/"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion (in REPL only)",
    example =
      """|lollypopComponents("com.lollypop.repl.Ls$")
         |ls -("./app/examples/")
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_SYSTEM_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns a dataframe containing a list of files matching any specified criterion (in REPL only).\nNOTE: ~ indicates the user's home directory.",
    example =
      """|lollypopComponents("com.lollypop.repl.Ls$")
         |ls ~(".")
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Ls] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Ls(p.expressions.get("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
