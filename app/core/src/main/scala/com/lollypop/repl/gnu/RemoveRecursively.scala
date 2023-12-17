package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.RemoveRecursively.keyword
import com.lollypop.runtime.RuntimeFiles.RecursiveFileList
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Recursively removes files or collections of files via pattern-matching.
 * @param path the [[Expression expression]] representing the path (e.g. './csv/monthly/2023/1105')
 * @example {{{
 *  rmr './csv/monthly/2023/1105'
 * }}}
 */
case class RemoveRecursively(path: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    path.pullFile ~>> {
      case d if !d.isDirectory => d.deleteRecursively()
      case _ => false
    }
  }

  override def toSQL: String = Seq(keyword, path.toSQL).mkString(" ")

}

object RemoveRecursively extends ExpressionParser {
  private val keyword = "rmr"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Recursively removes files or collections of files via pattern-matching.",
    example =
      """|val `count` =
         |  try
         |    cp 'build.sbt' 'temp.txt'
         |  catch e => -1
         |  finally
         |    rmr 'temp.txt'
         |
         |"{{`count`}} bytes copied.\n" ===> stdout
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[RemoveRecursively] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(RemoveRecursively(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
