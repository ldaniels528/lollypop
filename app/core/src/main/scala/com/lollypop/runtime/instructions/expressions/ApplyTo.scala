package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.TemplateProcessor.TokenStreamExtensions
import com.lollypop.language.models.{Expression, FunctionCall}
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.datatypes.Inferences.InstructionTyping
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.instructions.functions.FunctionArguments
import com.lollypop.runtime.{LollypopNative, Scope}
import lollypop.io.IOCost

/**
 * Apply-to (operator)
 * @example {{{
 *   ['A' to 'Z'](5) // 'F'
 * }}}
 * @example {{{
 *   sum(3, 6, 9) // 18
 * }}}
 * @example {{{
 *   'Hello World'(4) // 'o'
 * }}}
 * @example {{{
 *   @@stocks(12175) // { symbol: 'T', exchange: 'NYSE', lastSale: 22.77 }
 * }}}
 * @example {{{
 *   (x => x ** x)(5) // 3125
 * }}}
 */
case class ApplyTo(host: Expression, tuple: Expression) extends RuntimeExpression with FunctionCall with LollypopNative {

  override def args: List[Expression] = tuple match {
    case FunctionArguments(args) => args
  }

  override def execute()(implicit scope: Scope): (Scope, IOCost, Any) = {
    (scope, IOCost.empty, processInternalOps(host, args))
  }

  override def returnType: DataType = {
    host.returnType match {
      case StringType(_, _) => CharType
      case ArrayType(componentType, _) => componentType
      case _ => AnyType
    }
  }

  override def toSQL: String = host.wrapSQL + args.map(_.toSQL).mkString("(", ", ", ")")

}

object ApplyTo extends ExpressionChainParser {

  def apply(stream: TokenStream, host: Option[Expression])(implicit compiler: SQLCompiler): Option[ApplyTo] = {
    host.flatMap(parseExpressionChain(stream, _))
  }

  override def parseExpressionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[ApplyTo] = {
    if (understands(ts)) compiler.nextExpression(ts).map(ApplyTo(host, _)) else None
  }

  override def help: List[HelpDoc] = Nil

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.isPreviousTokenOnSameLine && ts.is("(")
  }
}