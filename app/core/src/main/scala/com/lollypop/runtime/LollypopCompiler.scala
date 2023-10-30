package com.lollypop.runtime

import com.lollypop.language.TemplateProcessor.{TagInstructionWithLineNumbers, TokenStreamExtensions}
import com.lollypop.language._
import com.lollypop.language.models._
import com.lollypop.runtime.instructions.conditions._
import com.lollypop.runtime.instructions.functions.AnonymousNamedFunction
import com.lollypop.util.OptionHelper.OptionEnrichment

import scala.collection.mutable
import scala.language.{existentials, postfixOps}

/**
 * Lollypop Compiler
 * @author lawrence.daniels@gmail.com
 */
class LollypopCompiler(val ctx: LollypopUniverse) extends SQLCompiler {

  override def nextExpression(stream: TokenStream,
                              expr0: Option[Expression] = None,
                              isDictionary: Boolean = false,
                              preventAliases: Boolean = false): Option[Expression] = {
    // attempt to parse an expression (expression, condition or queryable)
    val t0 = stream.peek
    val result0 = expr0.map(_.tag(t0)) match {
      case lit@Some(literal: BooleanLiteral) => doConditionB(stream, Some(literal)).map(c => c: Expression) ?? doExpressionB(stream, lit)
      case Some(condition: Condition) => ctx.getConditionToExpressionChain(stream, condition) ?? doConditionB(stream, Some(condition))
      case expr@Some(expression) => doConditionA(stream, expression).map(c => c: Expression) ?? doExpressionB(stream, expr)
      case None => doExpressionA(stream, isDictionary, preventAliases) ?? ctx.getQueryable(stream)
    }

    // apply any modifiers
    val result1 = result0.map(Expression(stream, _))

    // if it's a expression, continue (chaining) or exit.
    if (result1.isEmpty) expr0
    else if (stream.hasNext) nextExpression(stream, result1)
    else result1
  }

  private def doExpressionA(stream: TokenStream, isDictionary: Boolean, preventAliases: Boolean): Option[Expression] = {
    val t0 = stream.peek
    val result0: Option[Expression] = stream match {
      // is it a dictionary?
      case ts if ts is "{" => nextDictionaryOrCodeBlock(ts)
      // is it a dictionary key-value pair? (e.g. Content-Type: "application/json")
      case ts if isDictionary && ts.isText && ts.peekAhead(1).exists(_ is "-") && ts.peekAhead(3).exists(_ is ":") =>
        val name = new mutable.StringBuilder()
        while (ts.hasNext && ts.isnt(":")) name.append(ts.next().valueAsString)
        val (_, v) = (ts.expect(":"), nextExpression(ts) || ts.dieExpectedExpression())
        Option(v.as(name.toString()))
      // is it a key-value pair? (e.g. name: "Rasheed")
      case ts if !preventAliases && ts.isText && ts.peekAhead(1).exists(_ is ":") =>
        val (k, _, v) = (ts.next().valueAsString, ts.expect(":"), nextExpression(ts) || ts.dieExpectedExpression())
        Option(v.as(k))
      // is it a function?
      case ts if ctx.isInstruction(ts) => ctx.getExpression(ts)
      case ts if ts.isFunctionCall => FunctionCall.parseExpression(ts)
      case ts if ctx.isFunctionCall(ts) => Option(AnonymousNamedFunction(name = ts.next().valueAsString))
      // **** no keywords past this point ****
      case ts if ts.isKeyword => None
      // is it a literal or field?
      case ts => Literal.parseExpression(ts)
    }
    result0.map(_.tag(t0))
  }

  private def doExpressionB(stream: TokenStream, expression: Option[Expression]): Option[Expression] = {
    val t0 = stream.peek
    expression.flatMap(ctx.getExpressionChain(stream, _)).map(_.tag(t0))
  }

  private def doConditionA(stream: TokenStream, expression: Expression): Option[Condition] = {
    val t0 = stream.peek
    ctx.getExpressionToConditionChain(stream, expression).map(_.tag(t0))
  }

  private def doConditionB(stream: TokenStream, condition: Option[Condition]): Option[Condition] = {
    val t0 = stream.peek
    condition.flatMap(ctx.getConditionToConditionChain(stream, _)).map(_.tag(t0))
  }

}

/**
 * Lollypop Compiler Companion
 * @author lawrence.daniels@gmail.com
 */
object LollypopCompiler {

  /**
   * Creates an instance of the Lollypop compiler
   * @param ctx the [[LollypopUniverse compiler context]]
   * @return the [[LollypopCompiler compiler]]
   */
  def apply(ctx: LollypopUniverse = LollypopUniverse()): LollypopCompiler = new LollypopCompiler(ctx)

}