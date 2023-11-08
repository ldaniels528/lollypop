package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_ASYNC_REACTIVE, PARADIGM_REACTIVE}
import com.lollypop.language.models.Instruction
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Once instruction - invokes an instruction or set of instructions one-time only
 */
case class Once(code: Instruction) extends RuntimeInvokable {
  private val invoked = new AtomicBoolean(false)

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    if (invoked.compareAndSet(false, true)) code.execute(scope) else (scope, IOCost.empty, null)
  }

  override def toSQL: String = s"once ${code.toSQL}"
}

object Once extends InvokableParser {
  val template = "once %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "once",
    category = CATEGORY_ASYNC_REACTIVE,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Invokes an instruction or set of instructions one-time only",
    example =
      """|[1 to 5].foreach(n => {
         |  stdout <=== 'This happens every cycle {{n}}\n'
         |  once stdout <=== 'This happens once {{n}}\n'
         |})
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Once] = {
    if (understands(ts)) {
      val params = SQLTemplateParams(ts, template)
      Some(Once(code = params.instructions("code")))
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "once"

}