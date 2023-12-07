package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_IMPERATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{Condition, Instruction}
import com.lollypop.runtime.instructions.conditions.RuntimeCondition
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

import scala.annotation.tailrec

/**
 * WHILE statement
 * @param condition the given [[Condition condition]]
 * @param code      the given [[Instruction instruction]]
 * @example
 * {{{
 * while cnt < 10
 * begin
 *    print 'Hello World'
 *    set cnt = cnt + 1
 * end
 * }}}
 * @example
 * {{{
 * while cnt < 10 {
 *    print 'Hello World'
 *    cnt = cnt + 1
 * }
 * }}}
 */
case class WhileDo(condition: Condition, code: Instruction) extends RuntimeInvokable {

  override def execute()(implicit scope0: Scope): (Scope, IOCost, Any) = {
    @tailrec
    def recurse(s: Scope, c: IOCost, r: Any): (Scope, IOCost, Any) = {
      if (!s.isReturned && RuntimeCondition.isTrue(condition)(s)) {
        val (i, j, k) = code.execute(s)
        recurse(i, c ++ j, k)
      } else (s, c, r)
    }

    recurse(scope0, IOCost.empty, null)
  }

  override def toSQL: String = Seq("while", condition.toSQL, "do", code.toSQL).mkString(" ")

}

object WhileDo extends InvokableParser {
  val templateCard: String = "while %c:condition ?do %i:command"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "while",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = templateCard,
    description = "Repeats the `command` while the `expression` is true",
    example =
      """|var x = 0
         |var y = 1
         |while x < 5 do { x += 1; y *= x }
         |y
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[WhileDo] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, templateCard)
      Some(WhileDo(condition = params.conditions("condition"), code = params.instructions.get("command") match {
        case Some(ops) => ops
        case None => ts.dieExpectedInvokable()
      }))
    } else None
  }

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "while"

}