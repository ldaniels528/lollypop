package com.qwery.language

import com.qwery.language.models.Queryable

/**
 * Represents an Queryable Parser
 */
trait QueryableParser extends LanguageParser {

  def parseQueryable(ts: TokenStream)(implicit compiler: SQLCompiler): Queryable

}
