package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_REACTIVE}
import com.qwery.language._
import com.qwery.language.models.{Expression, Instruction}
import com.qwery.runtime.instructions.expressions.RuntimeExpression.RichExpression
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

import java.util.{Timer, TimerTask}

/**
 * after instruction
 * @param delay       the [[Expression delay interval]] of execution
 * @param instruction the [[Instruction command(s)]] to execute
 * @example {{{ after '2 seconds' { delete from @@entries where attachID is null } }}}
 */
case class After(delay: Expression, instruction: Instruction) extends RuntimeInvokable {
  private val timer = new Timer()

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    timer.schedule(new TimerTask {
      override def run(): Unit = QweryVM.execute(scope, instruction)
    }, (delay.asInterval || dieExpectedInterval()).toMillis)
    (scope, IOCost.empty, timer)
  }

  override def toSQL: String = s"after ${delay.toSQL} ${instruction.toSQL}"

}

object After extends InvokableParser {

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "after",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_REACTIVE,
    syntax = templateCard,
    description = "Schedules a one-time execution of command(s) after a specific delay period",
    example =
      """|var ticker = 5
         |after Interval('100 millis') { ticker += 3 }
         |import "java.lang.Thread"
         |Thread.sleep(Long(250))
         |ticker is 8
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): After = {
    val params = SQLTemplateParams(ts, templateCard)
    After(delay = params.expressions("delay"), instruction = params.instructions("command"))
  }

  val templateCard: String = "after %e:delay %N:command"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "after"

}