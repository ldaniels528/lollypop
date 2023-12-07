package com.lollypop.repl.gnu

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.repl.gnu.Copy.keyword
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost
import org.apache.commons.io.IOUtils

import java.io.{File, FileInputStream, FileOutputStream}

/**
 * Copies a source file or directory to a target.
 * @param source the source [[File file or directory]]
 * @param target the target [[File path]]
 * @example {{{
 *  cp 'test.csv' 'test1.csv'
 * }}}
 */
case class Copy(source: Expression, target: Expression) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Long) = {
    val (sa, ca, src) = source.pullFile
    val (sb, cb, dest) = target.pullFile(sa)
    (sb, ca ++ cb, new FileInputStream(src).use { in =>
      new FileOutputStream(dest).use(IOUtils.copyLarge(in, _))
    })
  }

  override def toSQL: String = Seq(keyword, source.toSQL, target.toSQL).mkString(" ")

}

object Copy extends ExpressionParser {
  private val keyword = "cp"
  private val templateCard = s"$keyword %e:source %e:target"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Copies a source file or directory to a target.",
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

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Copy] = {
    if (!understands(ts)) None else {
      val p = SQLTemplateParams(ts, templateCard)
      Some(Copy(source = p.expressions("source"), target = p.expressions("target")))
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}