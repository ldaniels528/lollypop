package com.lollypop.runtime.instructions.expressions

import com.lollypop.language.TemplateProcessor.TokenStreamExtensions
import com.lollypop.language.models.{Expression, FunctionCall}
import com.lollypop.language.{ExpressionChainParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.datatypes.Inferences.InstructionTyping
import com.lollypop.runtime.datatypes._
import com.lollypop.runtime.{LollypopNative, Scope}
import lollypop.io.IOCost

/**
 * Element-At (operator)
 * @example {{{
 *   ['A' to 'Z'][5] // 'F'
 * }}}
 * @example {{{
 *   'Hello World'[4] // 'o'
 * }}}
 * @example {{{
 *   @stocks[12175] // { symbol: 'T', exchange: 'NYSE', lastSale: 22.77 }
 * }}}
 */
case class ElementAt(host: Expression, args: List[Expression]) extends RuntimeExpression with FunctionCall with LollypopNative {

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

  override def toSQL: String = host.wrapSQL + args.map(_.toSQL).mkString("[", ", ", "]")

}

object ElementAt extends ExpressionChainParser {

  def apply(host: Expression, params: Expression*): ElementAt = ElementAt(host, params.toList)

  def apply(stream: TokenStream, host: Option[Expression])(implicit compiler: SQLCompiler): Option[ElementAt] = {
    host.flatMap(parseExpressionChain(stream, _))
  }

  override def help: List[HelpDoc] = Nil

  override def parseExpressionChain(stream: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[ElementAt] = {
    if (!host.isPrimitive && stream.isPreviousTokenOnSameLine && (stream nextIf "[")) {
      val result = compiler.nextExpression(stream).map(ElementAt(host, _))
      stream.expect("]")
      result
    } else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts.isPreviousTokenOnSameLine && (ts is "[")
  }

}