package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.{Atom, Instruction, LambdaFunction}
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.functions.AnonymousNamedFunction
import lollypop.io.IOCost

/**
 * Represents a variable assignment after applying a CODEC function
 * @param ref          the [[Atom variable]] for which to set
 * @param codec        the CODEC [[LambdaFunction function]]
 * @param initialValue the variable's initial input [[Instruction value]]
 * @example {{{
 *   let r : md5 = "the brown fox did what?"
 *   r // 63f685c9a4868d0bd2028c61b597547f
 * }}}
 */
case class Let(ref: Atom, codec: LambdaFunction, initialValue: Instruction) extends RuntimeInvokable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    val s = scope.withVariable(ref.name, codec, initialValue)
    (s, IOCost.empty, null)
  }

  override def toSQL: String = List("let", ref.toSQL, ":", codec.toSQL, "=", initialValue.toSQL).mkString(" ")
}

object Let extends InvokableParser {
  private val template: String = "let %a:ref : %a:codec = %i:value"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "let",
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_FUNCTIONAL,
    syntax = template,
    description = "Creates a variable that automatically applies a CODEC function when mutated.",
    example =
      """|base64 = (value: String) => value.getBytes().base64()
         |let b64 : base64 = "Hello"
         |b64
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Let = {
    val params = SQLTemplateParams(ts, template)
    Let(ref = params.atoms("ref"),
      codec = AnonymousNamedFunction(params.atoms("codec").name),
      initialValue = params.instructions("value"))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "let"
}
