package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.qwery.language._
import com.qwery.language.models.{Expression, Instruction}
import com.qwery.runtime.instructions.expressions.RuntimeExpression
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

import java.util.{Timer, TimerTask}

/**
 * every instruction
 * @param interval  the [[Expression frequency interval]] of execution
 * @param invokable the [[Instruction command(s)]] to execute
 * @example {{{ every '2 seconds' { delete from @@entries where attachID is null } }}}
 */
case class Every(interval: Expression, invokable: Instruction) extends RuntimeExpression {

  override def evaluate()(implicit scope: Scope): Timer = {
    val timer = new Timer()
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = QweryVM.execute(scope, invokable)._3
    }, 0L, (interval.asInterval || dieExpectedInterval()).toMillis)
    timer
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
