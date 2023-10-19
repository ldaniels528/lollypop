package com.qwery.runtime.instructions.functions

import com.qwery.implicits.{MagicBoolImplicits, MagicImplicits}
import com.qwery.language.HelpDoc.{CATEGORY_MISC, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Expression, FunctionCall}
import com.qwery.language.{SQLCompiler, SQLTemplateParams, TokenStream}
import com.qwery.runtime.instructions.functions.ScalarFunctionCall.ArgumentExtraction

/**
 * Abstract class for three-or-four-argument ([[Expression]]) [[FunctionCallParser function call parsers]]
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    an example of the function being called
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParserE3Or4(name: String,
                                       description: String,
                                       examples: List[String],
                                       category: String,
                                       paradigm: String)
  extends FunctionCallParser(name, description, examples, template = s"$name ( %e:expr1 , %e:expr2 , %e:expr3 ?, +?%e:expr4 )", category, paradigm) {

  def this(name: String, description: String, example: String, category: String = CATEGORY_MISC, paradigm: String = PARADIGM_FUNCTIONAL) =
    this(name, description, List(example), category, paradigm)

  def apply(expr1: Expression, expr2: Expression, expr3: Expression, expr4: Option[Expression]): InternalFunctionCall

  override def getFunctionCall(args: List[Expression]): Option[FunctionCall] = {
    (args.length >= 3 && args.length <= 4) ==> args.extract3or4 ~> { case (a, b, c, d) => apply(a, b, c, d) }
  }

  override def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = {
    val params = SQLTemplateParams(ts, template)
    Option(apply(params.expressions("expr1"), params.expressions("expr2"), params.expressions("expr3"), params.expressions.get("expr4")))
  }

}