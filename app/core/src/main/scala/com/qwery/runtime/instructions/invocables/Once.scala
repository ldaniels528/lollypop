package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_CONTROL_FLOW, PARADIGM_REACTIVE}
import com.qwery.language.models.Instruction
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.{QweryVM, Scope}
import qwery.io.IOCost

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Once instruction - invokes an instruction or set of instructions one-time only
 */
case class Once(code: Instruction) extends RuntimeInvokable {
  private val invoked = new AtomicBoolean(false)

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    if (invoked.compareAndSet(false, true)) QweryVM.execute(scope, code) else (scope, IOCost.empty, null)
  }

  override def toSQL: String = s"once ${code.toSQL}"
}

object Once extends InvokableParser {
  val template = "once %i:code"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "once",
    category = CATEGORY_CONTROL_FLOW,
    paradigm = PARADIGM_REACTIVE,
    syntax = template,
    description = "Invokes an instruction or set of instructions one-time only",
    example =
      """|[1 to 5].foreach(n => {
         |  out <=== 'This happens every cycle {{n}}\n'
         |  once out <=== 'This happens once {{n}}\n'
         |})
         |""".stripMargin
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Once = {
    val params = SQLTemplateParams(ts, template)
    Once(code = params.instructions("code"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "once"

}