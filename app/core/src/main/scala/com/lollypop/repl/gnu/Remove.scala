package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Remove.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Removes a file or a collection of files via pattern-matching.
 * @param path the [[Expression expression]] representing the path (e.g. 'transactions.csv')
 * @example {{{
 *  rm './csv/monthly/2023/1105/transactions.csv'
 * }}}
 */
case class Remove(path: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    path.pullFile map {
      case d if !d.isDirectory => d.delete()
      case _ => false
    }
  }

  override def toSQL: String = Seq(keyword, path.toSQL).mkString(" ")

}

object Remove extends ExpressionParser {
  private val keyword = "rm"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Removes a file or a collection of files via pattern-matching.",
    example =
      """|val `count` =
         |  try
         |    cp 'build.sbt' 'temp.txt'
         |  catch e => -1
         |  finally
         |    rm 'temp.txt'
         |
         |"{{`count`}} bytes copied.\n" ===> stdout
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Remove] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Remove(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}