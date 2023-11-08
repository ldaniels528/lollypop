package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{CodeBlock, Condition, Instruction}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import lollypop.io.IOCost

import scala.annotation.tailrec

/**
 * DO ... WHILE statement
 * @param code      the given [[Instruction instruction]]
 * @param condition the given [[Condition condition]]
 * @example
 * {{{
 * var cnt = 0
 * do {
 *    println 'Hello World'
 *    cnt += 1
 * } while cnt < 10
 * }}}
 */
case class DoWhile(code: Instruction, condition: Condition) extends RuntimeInvokable {

  override def execute()(implicit scope0: Scope): (Scope, IOCost, Any) = {
    @tailrec
    def recurse(scope: Scope): (Scope, IOCost, Any) = {
      val _code = code match {
        case CodeBlock(ops) => InlineCodeBlock(ops)
        case op => op
      }
      val (s, c, r) = _code.execute(scope)
      if (!s.isReturned && RuntimeCondition.isTrue(condition)(s)) recurse(s) else (s, c, r)
    }

    recurse(scope0)
  }

  override def toSQL: String = s"do ${code.toSQL} while ${condition.toSQL}"
}

object DoWhile extends InvokableParser {
  val templateCard: String = "do %i:command while %c:condition"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "do",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Creates a loop that executes enclosed statement(s) until the test condition evaluates to false",
    example =
      """|var x = 0
         |var y = 1
         |do { x += 1; y *= x } while x < 5
         |y
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[DoWhile] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(DoWhile(condition = params.conditions("condition"), code = params.instructions.get("command") match {
        case Some(ops) => ops
        case None => ts.dieExpectedInvokable()
      }))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "do"

}