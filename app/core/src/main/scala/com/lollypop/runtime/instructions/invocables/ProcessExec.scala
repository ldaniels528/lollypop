package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.Token.ProcessInvocationToken
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.{Scope, InlineVariableReplacement}
import lollypop.io.IOCost

import scala.sys.process.Process

/**
 * Invokes a native process from the host operating system
 * @param sourceCode the native code/instructions (e.g. 'iostat 1 5')
 * @example {{{
 *   (? iostat 1 5 ?)
 * }}}
 * @example {{{
 *   def iostat(n, m) := transpose(output: (? iostat $n $m ?))
 *   iostat(1, 5)
 * }}}
 * @example {{{
 *   def ps() := transpose(output: (? ps aux ?))
 *   from ps() limit 5
 * }}}
 */
case class ProcessExec(sourceCode: String) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, LazyList[String]) = {
    (scope, IOCost.empty, Process(sourceCode.replaceVariables()).lazyLines)
  }

  override def toSQL: String = s"(?$sourceCode?)"
}

object ProcessExec extends ExpressionParser {
  private val templateCard = "(? %e:command ?)"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "(?",
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Invokes a native process from the host operating system",
    example =
      """|def iostat(n, m) := transpose(output: (? iostat $n $m ?))
         |iostat(1, 5)
         |""".stripMargin,
    isExperimental = true
  ), HelpDoc(
    name = "(?",
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Invokes a native process from the host operating system",
    example =
      """|def ps() := transpose(output: (? ps aux ?))
         |from ps() limit 5
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ProcessExec] = {
    if (understands(ts)) {
      ts.next() match {
        case ProcessInvocationToken(_, code, _, _) => Some(ProcessExec(code))
        case x => ts.dieIllegalType(x)
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.peek match {
      case Some(t: ProcessInvocationToken) => t.id == "?"
      case _ => false
    }
  }

}

