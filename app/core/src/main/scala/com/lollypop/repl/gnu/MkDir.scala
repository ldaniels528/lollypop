package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.MkDir.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Creates a new directory.
 * @param pathExpr the [[Expression expression]] representing the directory (e.g. 'csv/monthly/2023/')
 * @example {{{
 *  mkdir csv/monthly/2023/1105
 * }}}
 */
case class MkDir(pathExpr: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    pathExpr.pullFile map (_.mkdirs())
  }

  override def toSQL: String = Seq(keyword, pathExpr.toSQL).mkString(" ")

}

object MkDir extends ExpressionParser {
  private val keyword = "mkdir"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates a new directory.",
    example =
      """|created = mkdir temp
         |response = iff(not created, "Couldn't do it", "Done")
         |rmdir temp
         |response
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[MkDir] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(MkDir(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}