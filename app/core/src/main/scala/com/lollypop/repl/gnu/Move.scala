package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Move.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import java.io.File

/**
 * Moves a source file or directory to a target.
 * @param source the source [[File file or directory]]
 * @param target the target [[File path]]
 * @example {{{
 *  mv 'test.csv' 'test1.csv'
 * }}}
 */
case class Move(source: Expression, target: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, src) = source.pullFile
    val (sb, cb, dest) = target.pullFile(sa)
    (sb, ca ++ cb, src.renameTo(dest))
  }

  override def toSQL: String = Seq(keyword, source.toSQL, target.toSQL).mkString(" ")

}

object Move extends ExpressionParser {
  private val keyword = "mv"
  private val templateCard = s"$keyword %e:source %e:target"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Renames a file or moves the file to a directory.",
    example =
      """|try {
         |  cp 'build.sbt' 'temp.txt'
         |  mv 'temp.txt' 'temp1.txt'
         |} catch e => {
         |  e ===> stderr
         |  false
         |} finally rm 'temp.txt' or rm 'temp1.txt'
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Move] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Move(source = p.expressions("source"), target = p.expressions("target")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
