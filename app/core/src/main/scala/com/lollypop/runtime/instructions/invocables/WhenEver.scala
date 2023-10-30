package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Expression, Instruction}
import com.lollypop.runtime.{Observable, Scope}
import lollypop.io.IOCost

/**
 * whenever instruction - executes an instruction at the moment the trigger condition evaluates as true
 * @param expression the trigger [[Expression expression]]
 * @param code       the [[Instruction instruction]] to execute
 * @example {{{ whenever n_bricks is 0 { out.println('n_bricks is empty') } }}}
 */
case class WhenEver(expression: Expression, code: Instruction) extends RuntimeInvokable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.withObservable(Observable(expression, code)), IOCost.empty, null)
  }

  override def toSQL: String = s"whenever ${expression.toSQL} ${code.toSQL}"
}

object WhenEver extends InvokableParser {
  val templateCard = "whenever %e:expr %i:code"

  override def help: List[HelpDoc] = List(
    HelpDoc(
      name = "whenever",
      category = CATEGORY_ASYNC_REACTIVE,
      paradigm = PARADIGM_REACTIVE,
      syntax = templateCard,
      description = "Executes an instruction at the moment the expression evaluates as true",
      example =
        """|whenever n_bricks is 0 { stdout <=== "n_bricks is empty\n" }
           |stdout <=== "Setting n_bricks to 0\n"
           |n_bricks = 0
           |stdout <=== "Did it work?"
           |""".stripMargin
    ), HelpDoc(
      name = "whenever",
      category = CATEGORY_ASYNC_REACTIVE,
      paradigm = PARADIGM_REACTIVE,
      syntax = templateCard,
      description = "Executes an instruction at the moment the expression evaluates as true",
      example =
        """|whenever '^set(.*)'
           |  stdout <=== "instruction was '{{__INSTRUCTION__}}'\n"
           |
           |set x = { message: "Confirmed" }
           |stdout <=== "Did it work?"
           |""".stripMargin
    ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): WhenEver = {
    val params = SQLTemplateParams(ts, templateCard)
    WhenEver(expression = params.expressions("expr"), code = params.instructions("code"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "whenever"
}
