package com.qwery.language

import com.qwery.language.models.Modifiable

trait ModifiableParser extends LanguageParser {

  def parse(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Modifiable] = {
    if (understands(ts)) Some(parseModifiable(ts)) else None
  }

  def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Modifiable

}
