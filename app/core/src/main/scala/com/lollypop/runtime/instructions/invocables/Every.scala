package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

import java.util.{Timer, TimerTask}

/**
 * every instruction
 * @param interval  the [[Expression frequency interval]] of execution
 * @param invokable the [[Instruction command(s)]] to execute
 * @example {{{ every '2 seconds' { delete from @@entries where attachID is null } }}}
 */
case class Every(interval: Expression, invokable: Instruction) extends RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Timer) = {
    val timer = new Timer()
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = LollypopVM.execute(scope, invokable)._3
    }, 0L, (interval.asInterval || dieExpectedInterval()).toMillis)
    (scope, IOCost.empty, timer)
  }

  override def toSQL: String = s"every ${interval.toSQL} ${invokable.toSQL}"

}

object Every extends ExpressionParser {
  private val template = "every %e:interval %i:command"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "every",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Schedules the execution of command(s) on a specific interval",
    example =
      """|var n = 0
         |val timer = every '20 millis' {
         |  n += 1
         |}
         |import "java.lang.Thread"
         |Thread.sleep(Long(1000))
         |timer.cancel()
         |n
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Every] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Option(Every(interval = params.expressions("interval"), invokable = params.instructions("command")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "every"

}
