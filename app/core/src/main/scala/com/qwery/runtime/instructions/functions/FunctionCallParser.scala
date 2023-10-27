package com.qwery.runtime.instructions.functions

import com.qwery.language.HelpDoc.{CATEGORY_UNCLASSIFIED, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Expression, FunctionCall}
import com.qwery.language.{HelpDoc, LanguageParser, SQLCompiler, TokenStream}

/**
 * Base class for all function call parsers
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    an example of the function being called
 * @param template    the template describing the function arguments
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParser(val name: String,
                                  val description: String,
                                  val examples: List[String],
                                  val template: String,
                                  val category: String = CATEGORY_UNCLASSIFIED,
                                  val paradigm: String = PARADIGM_FUNCTIONAL)
  extends LanguageParser {

  override def help: List[HelpDoc] = examples.map(example =>
    HelpDoc(
      name = name,
      category = category,
      paradigm = paradigm,
      syntax = template,
      description = description,
      example = example
    ))

  def getFunctionCall(args: List[Expression]): Option[FunctionCall]

  def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall]

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is name

}
