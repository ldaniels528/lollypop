package com.lollypop.language

import com.lollypop.language.models.Invokable

/**
 * Represents an Invokable Parser
 */
trait InvokableParser extends LanguageParser {

  def parseInvokable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Invokable]

}
