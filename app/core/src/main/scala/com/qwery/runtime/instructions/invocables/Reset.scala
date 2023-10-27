package com.qwery.runtime.instructions.invocables

import com.qwery.language.HelpDoc.{CATEGORY_SCOPE_SESSION, PARADIGM_IMPERATIVE}
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import qwery.io.IOCost

case class Reset() extends RuntimeInvokable {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope.getUniverse.createRootScope(), IOCost.empty, null)
  }

  override def toSQL: String = Reset.name
}

object Reset extends InvokableParser {
  val name = "reset"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = name,
    category = CATEGORY_SCOPE_SESSION,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = name,
    description = "Resets the scope; wiping out all state",
    example = name
  ))

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Reset = {
    if (ts.nextIf(name)) Reset() else ts.dieIllegalIdentifier()
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is name
}