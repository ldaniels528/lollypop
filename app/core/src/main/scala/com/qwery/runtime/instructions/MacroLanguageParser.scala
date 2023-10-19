package com.qwery.runtime.instructions

import com.qwery.language.{HelpDoc, LanguageParser, SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.expressions.MacroCall
import com.qwery.runtime.instructions.infrastructure.Macro

import scala.collection.concurrent.TrieMap

/**
 * Macro Language Parser
 * @example {{{
 *  macro "%e:x ²" := x ** x
 *  (8)² //=> 64
 * }}}
 */
object MacroLanguageParser extends LanguageParser {
  private val templates = TrieMap[String, Macro]()

  def apply(stream: TokenStream)(implicit compiler: SQLCompiler): Option[MacroCall] = {
    templates.find { case (template, _) => stream.matches(template) } map { case (template, _macro) =>
      val params = SQLTemplateParams(stream, template)
      MacroCall(_macro, params.consumables)
    }
  }

  override def help: List[HelpDoc] = Nil

  def registerMacro(model: Macro): Unit = templates(model.template) = model

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    templates.keySet.exists(ts.matches(_))
  }

}
