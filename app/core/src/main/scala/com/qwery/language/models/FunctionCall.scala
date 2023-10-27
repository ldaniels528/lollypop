package com.qwery.language.models

import com.qwery.language.TemplateProcessor.{TagInstructionWithLineNumbers, TokenStreamExtensions}
import com.qwery.language.{ExpressionParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.expressions.NamedFunctionCall

/**
 * Represents a function call
 */
trait FunctionCall extends Functional {

  def args: List[Expression]

}

object FunctionCall  extends ExpressionParser {

  override def help: List[HelpDoc] = Nil

  /**
   * Parses an internal or user-defined (e.g. "transpose(new `java.util.Date`())")
   * @param stream the given [[TokenStream token stream]]
   * @return a [[Expression function]]
   */
  override def parseExpression(stream: TokenStream)(implicit compiler: SQLCompiler): Option[Expression] = {
    val t0 = stream.peek
    val result0 = stream match {
      // is it a selection function?
      case ts if compiler.ctx.isFunctionCall(ts) => compiler.ctx.getFunctionCall(ts)
      // must be a user-defined function
      case ts => Option(NamedFunctionCall(name = ts.next().valueAsString, compiler.nextListOfArguments(ts)))
    }
    result0.map(_.tag(t0))
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts.isFunctionCall

}