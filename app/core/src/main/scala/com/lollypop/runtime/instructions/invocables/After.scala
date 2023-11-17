package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONCURRENCY, PARADIGM_REACTIVE}
import com.lollypop.language._
import com.lollypop.language.models.{ConcurrentInstruction, Expression, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.conversions.ExpressiveTypeConversion
import lollypop.io.IOCost

import java.util.{Timer, TimerTask}

/**
 * After statement
 * @param delay       the [[Expression delay interval]] of execution
 * @param instruction the [[Instruction command(s)]] to execute
 * @example {{{
 *  after Duration('2 seconds')
 *    delete from @entries where attachID is null
 * }}}
 */
case class After(delay: Expression, instruction: Instruction)
  extends RuntimeInvokable with ConcurrentInstruction {
  private val timer = new Timer()

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (sa, ca, _delay) = delay.pullDuration
    timer.schedule(new TimerTask {
      override def run(): Unit = instruction.execute(scope)
    }, _delay.toMillis)
    (sa, ca, timer)
  }

  override def toSQL: String = s"after ${delay.toSQL} ${instruction.toSQL}"

}

object After extends InvokableParser {
  private val templateCard: String = "after %e:delay %N:command"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "after",
    category = CATEGORY_CONCURRENCY,
    paradigm = PARADIGM_REACTIVE,
    syntax = templateCard,
    description = "Schedules a one-time execution of command(s) after a specific delay period",
    example =
      """|var ticker = 5
         |after Duration('100 millis') { ticker += 3 }
         |import "java.lang.Thread"
         |Thread.sleep(Long(250))
         |ticker is 8
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[After] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(After(delay = params.expressions("delay"), instruction = params.instructions("command")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "after"

}