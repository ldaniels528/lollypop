package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Echo.keyword
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

/**
 * Prints text to the standard output.
 * @param text the [[Expression expression]] representing the text to print.
 * @example {{{
 *  echo 'Hello World'
 * }}}
 */
case class Echo(text: Expression) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Unit) = {
    val (sa, ca, _text) = text.pullString
    sa.stdOut.println(_text)
    (sa, ca, ())
  }

  override def toSQL: String = Seq(keyword, text.toSQL).mkString(" ")

}

object Echo extends ExpressionParser {
  private val keyword = "echo"
  private val templateCard = s"$keyword %e:text"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Prints text to the standard output.",
    example =
      """|echo "Hello Lollypop!"
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Prints text to the standard output.",
    example =
      """|item = { symbol: "ABC", lastSale: 97.55 }
         |echo '{{item.symbol}} is {{item.lastSale}}/share'
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Echo] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Echo(p.expressions("text")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}