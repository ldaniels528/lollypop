package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.qwery.language.models.Expression
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.invocables.RuntimeInvokable
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * Throws a JVM exception
 * @param error an instance of [[Throwable throwable]]
 * @example {{{ throw new IllegalArgumentException("Usage: RapFileGenerator <rap-file-name> <rap-hex-code>") }}}
 */
case class ThrowException(error: Expression) extends RuntimeInvokable with Expression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    QweryVM.execute(scope, error)._3 match {
      case cause: Throwable => throw cause
      case cause: String => this.die(cause)
      case x => error.dieIllegalType(x)
    }
  }

  override def toSQL: String = s"throw ${error.toSQL}"
}

object ThrowException extends InvokableParser {
  private val templateCard = "throw %e:error"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "throw",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Throws a JVM exception",
    example =
      """|try
         |  throw new `java.lang.RuntimeException`('A processing error occurred')
         |catch e => out <=== e.getMessage()
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): ThrowException = {
    val params = SQLTemplateParams(ts, templateCard)
    ThrowException(params.expressions("error"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "throw"
}