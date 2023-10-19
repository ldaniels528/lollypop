package com.qwery.language

import com.qwery.language.IfNotExists.IfNotExistsTemplateTag
import com.qwery.language.TemplateProcessor.tags.TemplateTag

trait IfNotExists { self : LanguageParser =>

  // add custom tag for if not exists (e.g. "%IFNE:exists" => "if not exists")
  TemplateProcessor.addTag("IFNE", IfNotExistsTemplateTag)

}

object IfNotExists {

  def nextIfNotExists(stream: TokenStream): Boolean = {
    stream match {
      case ts if ts nextIf "if not exists" => true
      case ts => ts.dieExpectedIfNotExists()
    }
  }

  case class IfNotExistsTemplateTag(name: String) extends TemplateTag {
    override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
      SQLTemplateParams(indicators = Map(name -> nextIfNotExists(stream)))
    }

    override def toCode: String = s"%IFNE:$name"
  }

}