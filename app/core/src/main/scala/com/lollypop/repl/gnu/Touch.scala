package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Touch.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import java.io.RandomAccessFile
import scala.util.Try

/**
 * Creates or updates the last modified time of a file.
 * @param expression the optional [[Expression expression]] representing the path (e.g. './lollypop_db')
 * @example {{{
 *   touch 'README.md'
 * }}}
 */
case class Touch(expression: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    expression.pullFile ~>> { file =>
      Try(new RandomAccessFile(file, "rw").close()).isSuccess
    }
  }

  override def toSQL: String = Seq(keyword, expression.toSQL).mkString(" ")
}

object Touch extends ExpressionParser {
  private val keyword = "touch"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates or updates the last modified time of a file. Return true if successful.",
    example =
      """|touch 'app/examples/src/main/resources/log4j.properties'
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Touch] = {
    if (understands(ts)) {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Touch(p.expressions("path")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
