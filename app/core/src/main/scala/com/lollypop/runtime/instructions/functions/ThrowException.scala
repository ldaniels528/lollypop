package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.invocables.RuntimeInvokable
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Throws a JVM exception
 * @param error an instance of [[Throwable throwable]]
 * @example {{{ throw new IllegalArgumentException("Usage: RapFileGenerator <rap-file-name> <rap-hex-code>") }}}
 */
case class ThrowException(error: Expression) extends RuntimeInvokable with Expression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    LollypopVM.execute(scope, error)._3 match {
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
         |catch e => stdout <=== e.getMessage()
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ThrowException] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(ThrowException(params.expressions("error")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "throw"
}