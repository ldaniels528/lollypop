package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.qwery.language._
import com.qwery.language.models.{Condition, Expression, Instruction}
import com.qwery.runtime.instructions.conditions.RuntimeCondition.isTrue
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

/**
 * If the condition evaluates to true, then `onTrue` is executed; otherwise `onFalse` is executed.
 * @param condition the [[Condition condition]] to evaluate
 * @param onTrue    the [[Instruction]] to execute when the result of the condition is true
 * @param onFalse   the [[Instruction]] to execute when the result of the condition is false
 * @example if(@value > 5) { call doThis(value) } else { call doThat(value) }
 */
case class IF(condition: Condition, onTrue: Instruction, onFalse: Option[Instruction])
  extends RuntimeInvokable with Expression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    @inline def run(op: Instruction): (Scope, IOCost, Any) = QweryVM.execute(scope, op)

    if (isTrue(condition)) run(onTrue) else onFalse.map(run).getOrElse((scope, IOCost.empty, null))
  }

  override def toSQL: String = {
    ("if(" :: condition.toSQL :: ") " :: onTrue.toSQL :: onFalse.map(i => " else " + i.toSQL).toList).mkString
  }
}

object IF extends InvokableParser {
  val templateCard = "if %c:condition %i:onTrue ?else +?%i:onFalse"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "if",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "If the `expression` is true, then `outcomeA` otherwise `outcomeB`",
    example =
      """|value = 123
         |if(value > 99) "Yes!" else "No."
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): IF = {
    val params = SQLTemplateParams(ts, templateCard)
    IF(
      condition = params.conditions("condition"),
      onTrue = params.instructions("onTrue"),
      onFalse = params.instructions.get("onFalse"))
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "if"

}
