package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.RmDir.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Removes a specific directory.
 * @param pathExpr the [[Expression expression]] representing the directory (e.g. 'csv/monthly/2023/')
 * @example {{{
 *  rmdir csv/monthly/2023/1105
 * }}}
 */
case class RmDir(pathExpr: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    pathExpr.pullFile map {
      case d if d.isDirectory => d.delete()
      case _ => false
    }
  }

  override def toSQL: String = Seq(keyword, pathExpr.toSQL).mkString(" ")

}

object RmDir extends ExpressionParser {
  private val keyword = "rmdir"
  private val templateCard = s"$keyword %e:path"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Removes a specific directory.",
    example =
      """|created = mkdir temp_zzz
         |response = iff(not created, "Couldn't do it", "Done")
         |rmdir temp_zzz
         |response
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[RmDir] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(RmDir(p.expressions("path")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}


