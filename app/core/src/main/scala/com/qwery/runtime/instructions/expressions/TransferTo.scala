package com.qwery.runtime.instructions.expressions

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_SYNC_IO, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.Streamable.{getInputStream, getOutputStream}
import com.qwery.runtime.instructions.expressions.TransferTo._symbol
import com.qwery.util.ResourceHelper.AutoClose
import org.apache.commons.io.IOUtils

import java.io.OutputStream

/**
 * Transfer-To operator
 * @param a the [[Expression source expression]]
 * @param b the [[Expression target expression]]
 * @example {{{
 * import "java.io.File"
 * f = new File("/Users/ldaniels/.qweryrc")
 * f ===> out
 * }}}
 * @example {{{
 * import "java.lang.Thread"
 * val total = 100
 * val progressBarWidth = 50
 * [0 to total].foreach(i => {
 *    val progress = Int((Double(i) / total) * progressBarWidth)
 *    val progressBar = "[" + ("*" * progress) + (" " * (progressBarWidth - progress)) + "] {{i}}%"
 *    "\r{{progressBar}}" ===> out
 *    Thread.sleep(Long(100)) // Simulate some work being done
 * })
 * }}}
 */
case class TransferTo(a: Expression, b: Expression) extends RuntimeExpression {
  override def evaluate()(implicit scope: Scope): OutputStream = {
    getInputStream(a).use(in =>
      getOutputStream(b) ~> { out =>
        IOUtils.copy(in, out)
        out
      })
  }

  override def toSQL: String = Seq(a.toSQL, _symbol, b.toSQL).mkString(" ")

}

object TransferTo extends ExpressionChainParser {
  private val _symbol = "===>"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = _symbol,
    category = CATEGORY_SYNC_IO,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"a ${_symbol} b",
    description = "A declarative way to write to OutputStream or Writer resources",
    example =
      """|import "java.io.File"
         |f = new File("./test.json")
         |f ===> out
         |""".stripMargin
  ))

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[TransferTo] = {
    stream match {
      case ts if ts nextIf _symbol => for (b <- compiler.nextExpression(ts)) yield TransferTo(host, b)
      case _ => None
    }
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is _symbol

}