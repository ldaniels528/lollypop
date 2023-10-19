package com.qwery.language

import com.qwery.language.models.Invokable

/**
 * Represents an Invokable Parser
 */
trait InvokableParser extends LanguageParser {

  def parse(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Invokable] = {
    if (understands(ts)) Some(parseInvokable(ts)) else None
  }

  def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Invokable

}
