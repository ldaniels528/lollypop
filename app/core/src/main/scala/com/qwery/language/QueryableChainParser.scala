package com.qwery.language

import com.qwery.language.models.Queryable

/**
 * Represents an Queryable Chain Parser
 */
trait QueryableChainParser extends LanguageParser {

  def parseQueryableChain(ts: TokenStream, host: Queryable)(implicit compiler: SQLCompiler): Queryable

}
