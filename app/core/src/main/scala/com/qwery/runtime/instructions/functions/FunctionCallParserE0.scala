package com.qwery.runtime.instructions.functions

import com.qwery.implicits.MagicBoolImplicits
import com.qwery.language.HelpDoc.{CATEGORY_MISC, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Expression, FunctionCall}
import com.qwery.language.{SQLCompiler, SQLTemplateParams, TokenStream}

/**
 * Abstract class for zero-argument [[FunctionCallParser function call parsers]]
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    optional examples of the function being called
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParserE0(name: String, description: String, examples: List[String], category: String, paradigm: String)
  extends FunctionCallParser(name, description, examples, template = s"$name ( )", category, paradigm) {

  /**
   * Abstract class for zero-argument [[FunctionCallParser function call parsers]]
   * @param name        the name of the function being called
   * @param description the description of the function being called
   * @param example     an example of the function being called
   * @param category    the instruction category (e.g. "Default")
   * @param paradigm    the instruction paradigm (e.g. "Declarative")
   */
  def this(name: String, description: String, example: String, category: String = CATEGORY_MISC, paradigm: String = PARADIGM_FUNCTIONAL) = {
    this(name, description, List(example), category, paradigm)
  }

  def apply(): InternalFunctionCall

  override def getFunctionCall(args: List[Expression]): Option[FunctionCall] = args.isEmpty ==> apply()

  override def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = {
    SQLTemplateParams(ts, template)
    Option(apply())
  }

}
