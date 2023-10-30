package com.lollypop.runtime.instructions.functions

import com.lollypop.implicits.MagicBoolImplicits
import com.lollypop.language.HelpDoc.{CATEGORY_SYSTEM_TOOLS, PARADIGM_FUNCTIONAL}
import com.lollypop.language.models.{Expression, FunctionCall}
import com.lollypop.language.{SQLCompiler, SQLTemplateParams, TokenStream}

/**
 * Abstract class for zero-or-one-argument ([[Expression]]) [[FunctionCallParser function call parsers]]
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    an example of the function being called
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParserE0Or1(name: String, description: String, examples: List[String], category: String, paradigm: String)
  extends FunctionCallParser(name, description, examples, template = s"$name ( ?%e:expr )", category, paradigm) {

  def this(name: String, description: String, example: String, category: String = CATEGORY_SYSTEM_TOOLS, paradigm: String = PARADIGM_FUNCTIONAL) =
    this(name, description, List(example), category, paradigm)

  def apply(expr: Option[Expression]): InternalFunctionCall

  override def getFunctionCall(args: List[Expression]): Option[FunctionCall] = {
    (args.length >= 0 && args.length <= 1) ==> apply(args.headOption)
  }

  override def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = {
    val params = SQLTemplateParams(ts, template)
    Option(apply(params.expressions.get("expr")))
  }

}