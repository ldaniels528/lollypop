package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.repl.gnu.Pwd.keyword
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Print Working Directory (PWD)
 */
case class Pwd() extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, String) = {
    (scope, IOCost.empty, getCWD)
  }

  override def toSQL: String = keyword
}

object Pwd extends ExpressionParser {
  private val keyword = "pwd"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = keyword,
    description = "Retrieves the contents of a file.",
    example =
      """|cd ~
         |pwd
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    if (!ts.nextIf(keyword)) None else Some(Pwd())
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}