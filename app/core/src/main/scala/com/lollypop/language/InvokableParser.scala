package com.lollypop.language

import com.lollypop.language.models.Invokable

/**
 * Represents an Invokable Parser
 */
trait InvokableParser extends LanguageParser {

  def parse(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Invokable] = {
    if (understands(ts)) parseInvokable(ts) else None
  }

  def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Invokable]

}
