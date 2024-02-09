package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONCURRENCY, PARADIGM_REACTIVE}
import com.lollypop.language._
import com.lollypop.language.models.{ContainerInstruction, Expression, Instruction}
import com.lollypop.runtime._
import com.lollypop.runtime.instructions.RuntimeInstruction
import lollypop.io.IOCost

import java.util.{Timer, TimerTask}

/**
 * Every statement
 * @param interval the [[Expression frequency interval]] of execution
 * @param code     the [[Instruction instruction]] to execute
 * @example {{{
 *  every Duration('2 seconds')
 *    delete from @entries where attachID is null
 * }}}
 */
case class Every(interval: Expression, code: Instruction)
  extends Expression with RuntimeInstruction with ContainerInstruction {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Timer) = {
    val (sa, ca, _interval) = interval.pullDuration
    val timer = new Timer()
    timer.scheduleAtFixedRate(new TimerTask {
      override def run(): Unit = code.execute(scope)
    }, 0L, _interval.toMillis)
    (sa, ca, timer)
  }

  override def toSQL: String = s"every ${interval.toSQL} ${code.toSQL}"

}

object Every extends ExpressionParser {
  private val template = "every %e:interval %i:command"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "every",
    category = CATEGORY_CONCURRENCY,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Schedules the execution of command(s) on a specific interval",
    example =
      """|var n = 0
         |val timer = every Duration("20 millis") n += 1
         |import "java.lang.Thread"
         |Thread.sleep(Long(1000))
         |timer.cancel()
         |n
         |""".stripMargin
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Every] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Option(Every(interval = params.expressions("interval"), code = params.instructions("command")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "every"

}
