package com.qwery.runtime.instructions.invocables

import com.qwery.language.models.Invokable
import com.qwery.language.{HelpDoc, InvokableParser, SQLCompiler, TokenStream}
import com.qwery.runtime.Scope
import qwery.io.IOCost

case object EOL extends RuntimeInvokable with InvokableParser {

  override def help: List[HelpDoc] = Nil

  override def invoke()(implicit scope: Scope): (Scope, IOCost, Any) = (scope, IOCost.empty, null)

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Invokable = {
    ts.expect(";")
    EOL
  }

  override def toSQL: String = ";"

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is ";"
}

