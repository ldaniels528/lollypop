package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.WordCount.keyword
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.util.ResourceHelper.AutoClose
import lollypop.io.IOCost

import scala.io.Source

/**
 * Returns the count of lines of a text file.
 * @param pathExpr the [[Expression expression]] representing the file (e.g. './csv/monthly/2023/1105/transactions.csv')
 * @example {{{
 *  wc './csv/monthly/2023/1105/transactions.csv'
 * }}}
 */
case class WordCount(pathExpr: Expression) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Int) = {
    pathExpr.pullFile ~>> { file =>
      var count = 0
      Source.fromFile(file).use(_.foreach(_ => count += 1))
      count
    }
  }

  override def toSQL: String = Seq(keyword, pathExpr.toSQL).mkString(" ")

}

object WordCount extends ExpressionParser {
  private val keyword = "wc"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Returns the count of lines of a text file.",
    example =
      """|wc LICENSE
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[WordCount] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(WordCount(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
