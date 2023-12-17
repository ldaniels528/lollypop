package com.lollypop.repl

import com.lollypop.language.HelpDoc.{CATEGORY_REPL_TOOLS, PARADIGM_DECLARATIVE}
import com.lollypop.language.Token.ProcessInvocationToken
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import lollypop.io.IOCost

import scala.sys.process.{Process, ProcessLogger}

/**
 * Represents a fully orchestrated process
 * @param code the code to execute
 * @example {{{
 *   (& iostat 1 5 &)
 * }}}
 */
case class ProcessPuppet(code: String) extends RuntimeExpression {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Process) = {
    (scope, IOCost.empty, Process(code).run(new ProcessLogger {
      override def out(s: => String): Unit = scope.stdOut.println(s)

      override def err(s: => String): Unit = scope.stdErr.println(s)

      override def buffer[T](f: => T): T = f
    }))
  }

  override def toSQL: String = s"(&$code&)"
}

object ProcessPuppet extends ExpressionParser {
  private val templateCard = "(& %e:command &)"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "(&",
    category = CATEGORY_REPL_TOOLS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Executes an application from the host operating system",
    example =
      """|(& iostat 1 3 &)
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[ProcessPuppet] = {
    if (understands(ts)) {
      ts.next() match {
        case ProcessInvocationToken(_, code, _, _) => Some(ProcessPuppet(code))
        case x => ts.dieIllegalType(x)
      }
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.peek match {
      case Some(t: ProcessInvocationToken) => t.id == "&"
      case _ => false
    }
  }

}
