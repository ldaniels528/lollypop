package com.qwery.runtime.instructions.functions

import com.qwery.implicits.{MagicBoolImplicits, MagicImplicits}
import com.qwery.language.HelpDoc.{CATEGORY_UNCLASSIFIED, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Expression, FunctionCall}
import com.qwery.language.{SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.functions.ScalarFunctionCall.ArgumentExtraction

/**
 * Abstract class for one-or-two-argument ([[Expression]]) [[FunctionCallParser function call parsers]]
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    an example of the function being called
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParserE1Or2(name: String,
                                       description: String,
                                       examples: List[String],
                                       category: String,
                                       paradigm: String)
  extends FunctionCallParser(name, description, examples, template = s"$name ( %e:expr1 ?, +?%e:expr2 )", category, paradigm) {

  def this(name: String, description: String, example: String, category: String = CATEGORY_UNCLASSIFIED, paradigm: String = PARADIGM_FUNCTIONAL) =
    this(name, description, List(example), category, paradigm)

  def apply(expr1: Expression, expr2: Option[Expression]): InternalFunctionCall

  override def getFunctionCall(args: List[Expression]): Option[FunctionCall] = {
    (args.length >= 2 && args.length <= 3) ==> args.extract1or2 ~> { case (a, b) => apply(a, b) }
  }

  override def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = {
    val params = SQLTemplateParams(ts, template)
    Option(apply(params.expressions("expr1"), params.expressions.get("expr2")))
  }

}