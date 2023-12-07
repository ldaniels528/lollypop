package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.expressions.TransferFrom._symbol
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost
import org.apache.commons.io.IOUtils

import java.io.OutputStream

/**
 * Transfer-From operator
 * @param a the [[Expression target expression]]
 * @param b the [[Expression source expression]]
 * @example {{{
 * import "java.io.File"
 * f = new File("/Users/ldaniels/.lollypoprc")
 * f <=== "Hello World"
 * }}}
 * @example {{{
 * import "java.lang.Thread"
 * val total = 100
 * val progressBarWidth = 50
 * [0 to total].foreach(i => {
 *    val progress = Int((Double(i) / total) * progressBarWidth)
 *    val progressBar = "[" + ("*" * progress) + (" " * (progressBarWidth - progress)) + "] {{i}}%"
 *    out <=== "\r{{progressBar}}"
 *    Thread.sleep(Long(100)) // Simulate some work being done
 * })
 * }}}
 */
case class TransferFrom(a: Expression, b: Expression) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, OutputStream) = {
    val (sa, ca, in) = b.pullInputStream
    val (sb, cb, out) = a.pullOutputStream(sa)
    (sb, ca ++ cb, in.use { _ =>
      IOUtils.copy(in, out)
      out
    })
  }

  override def toSQL: String = Seq(a.toSQL, _symbol, b.toSQL).mkString(" ")

}

object TransferFrom extends ExpressionChainParser {
  private val _symbol = "<==="

  override def help: List[HelpDoc] = List(HelpDoc(
    name = _symbol,
    category = CATEGORY_TRANSFORMATION,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"a ${_symbol} b",
    description = "A declarative way to write to OutputStream or Writer resources",
    example =
      """|import "java.io.File"
         |f = new File("./test1.json")
         |f <=== "Hello World\n"
         |f ===> stdout
         |""".stripMargin
  ))

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[TransferFrom] = {
    stream match {
      case ts if ts nextIf _symbol => for (b <- compiler.nextExpression(ts)) yield TransferFrom(host, b)
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}
