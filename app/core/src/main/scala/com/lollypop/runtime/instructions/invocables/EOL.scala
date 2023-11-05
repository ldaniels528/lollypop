package com.lollypop.runtime.instructions.invocables

import com.lollypop.language.models.Invokable
import com.lollypop.language.{HelpDoc, InvokableParser, SQLCompiler, TokenStream}
import com.lollypop.runtime.Scope
import lollypop.io.IOCost

/**
 * End of Line/Statement
 */
case object EOL extends RuntimeInvokable with InvokableParser {

  override def help: List[HelpDoc] = Nil

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = (scope, IOCost.empty, null)

  override def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Invokable] = {
    if (understands(ts)) {
      ts.expect(";")
      Some(EOL)
    } else None
  }

  override def toSQL: String = ";"

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is ";"
}

