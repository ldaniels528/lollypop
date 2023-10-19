package com.qwery.runtime.instructions.functions

import com.qwery.implicits.{MagicBoolImplicits, MagicImplicits}
import com.qwery.language.HelpDoc.{CATEGORY_MISC, PARADIGM_FUNCTIONAL}
import com.qwery.language.models.{Column, Expression, FunctionCall}
import com.qwery.language.{SQLCompiler, SQLTemplateParams, TokenStream, dieIllegalType}
import com.qwery.runtime.instructions.functions.ScalarFunctionCall.ArgumentExtraction

/**
 * Abstract class for multi-argument [[FunctionCallParser function call parsers]]
 * @param name        the name of the function being called
 * @param description the description of the function being called
 * @param examples    an example of the function being called
 * @param category    the instruction category (e.g. "Default")
 * @param paradigm    the instruction paradigm (e.g. "Declarative")
 */
abstract class FunctionCallParserP(name: String,
                                   description: String,
                                   examples: List[String],
                                   category: String,
                                   paradigm: String)
  extends FunctionCallParser(name, description, examples, template = s"$name %FP:params", category, paradigm) {

  def this(name: String, description: String, example: String, category: String = CATEGORY_MISC, paradigm: String = PARADIGM_FUNCTIONAL) =
    this(name, description, List(example), category, paradigm)

  def apply(params: List[Column]): InternalFunctionCall

  override def getFunctionCall(args: List[Expression]): Option[FunctionCall] = {
    args.length == 1 ==> args.extract1 ~> {
      case pb: ParameterBlock => apply(pb.parameters.map(_.toColumn))
      case xx => dieIllegalType(xx)
    }
  }

  override def parseFunctionCall(ts: TokenStream)(implicit compiler: SQLCompiler): Option[FunctionCall] = {
    val params = SQLTemplateParams(ts, template)
    Option(apply(params.parameters("params").map(_.toColumn)))
  }

}
