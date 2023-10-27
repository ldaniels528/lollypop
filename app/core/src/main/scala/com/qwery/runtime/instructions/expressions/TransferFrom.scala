package com.qwery.runtime.instructions.expressions

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_TRANSFORMATION, PARADIGM_DECLARATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import com.qwery.runtime.instructions.Streamable.{getInputStream, getOutputStream}
import com.qwery.runtime.instructions.expressions.TransferFrom._symbol
import com.qwery.util.ResourceHelper.AutoClose
import org.apache.commons.io.IOUtils

import java.io.OutputStream

/**
 * Transfer-From operator
 * @param a the [[Expression target expression]]
 * @param b the [[Expression source expression]]
 * @example {{{
 * import "java.io.File"
 * f = new File("/Users/ldaniels/.qweryrc")
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
  override def evaluate()(implicit scope: Scope): OutputStream = {
    getInputStream(b).use(in =>
      getOutputStream(a) ~> { out =>
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
         |f ===> out
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
