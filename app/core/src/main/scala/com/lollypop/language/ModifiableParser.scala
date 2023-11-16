package com.lollypop.language

import com.lollypop.language.models.Modifiable

trait ModifiableParser extends LanguageParser {

  def parseModifiable(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Modifiable]

}
