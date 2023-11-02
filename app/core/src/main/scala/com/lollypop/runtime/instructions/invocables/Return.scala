package com.lollypop.runtime.instructions.invocables

import com.lollypop.implicits.MagicImplicits
import com.lollypop.language.HelpDoc.CATEGORY_CONTROL_FLOW
import com.lollypop.language._
import com.lollypop.language.models.Instruction
import com.lollypop.runtime.{LollypopVM, Scope}
import com.lollypop.util.OptionHelper.OptionEnrichment
import lollypop.io.IOCost

/**
 * return statement
 * @param value the given return value
 * @example {{{ return @@rowSet }}}
 */
case class Return(value: Option[Instruction] = None) extends RuntimeInvokable {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val (s, c, r) = value.map(op => LollypopVM.execute(scope, op)) || (scope, IOCost.empty, null)
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

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Return] = {
    if (understands(ts)) {
      val value_? = SQLTemplateParams(ts, templateCard) ~> { params => params.instructions.get("value") }
      Some(Return(value_?))
    } else None
  }

  val templateCard: String = "return ?%i:value"

  override def understands(stream: TokenStream)(implicit compiler: SQLCompiler): Boolean = stream is "return"

}
