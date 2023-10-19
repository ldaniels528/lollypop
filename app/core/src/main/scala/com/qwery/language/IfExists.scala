package com.qwery.language

import com.qwery.language.IfExists.IfExistsTemplateTag
import com.qwery.language.TemplateProcessor.tags.TemplateTag

trait IfExists { self : LanguageParser =>

  // add custom tag for if exists (e.g. "%IFE:exists" => "if exists")
  TemplateProcessor.addTag("IFE", IfExistsTemplateTag)

}

object IfExists {

  def nextIfExists(stream: TokenStream): Boolean = {
    stream match {
      case ts if ts nextIf "if exists" => true
      case ts => ts.dieExpectedIfExists()
    }
  }

  case class IfExistsTemplateTag(name: String) extends TemplateTag {
    override def extract(stream: TokenStream)(implicit compiler: SQLCompiler): SQLTemplateParams = {
      SQLTemplateParams(indicators = Map(name -> nextIfExists(stream)))
    }

    override def toCode: String = s"%IFE:$name"
  }

}