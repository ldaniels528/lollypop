package com.qwery.runtime.instructions.invocables

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.CATEGORY_CONTROL_FLOW
import com.qwery.language._
import com.qwery.language.models.Instruction
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment
import qwery.io.IOCost

/**
 * return statement
 * @param value the given return value
 * @example {{{ return @@rowSet }}}
 */
case class Return(value: Option[Instruction] = None) extends RuntimeInvokable {

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (s, c, r) = value.map(op => QweryVM.execute(scope, op)) || (scope, IOCost.empty, null)
    (s.withReturned(isReturned = true), c, r)
  }

  override def toSQL: String = value.map(v => s"return ${v.toSQL}") getOrElse "return"

}

object Return extends InvokableParser {

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "return",
    category = CATEGORY_CONTROL_FLOW,
    syntax = templateCard,
    description = "Returns a result set (from a daughter scope)",
    example = "return 'Hello World'"
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Return = {
    val value_? = SQLTemplateParams(ts, templateCard) ~> { params => params.instructions.get("value") }
    Return(value_?)
  }

  val templateCard: String = "return ?%i:value"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "return"

}
