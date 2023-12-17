package com.lollypop.language.models

import com.lollypop.language.{SQLCompiler, TokenStream}

import scala.annotation.tailrec
import scala.language.implicitConversions

/**
 * Represents an expression; which in its simplest form is a value (boolean, number, string, etc.)
 * @author lawrence.daniels@gmail.com
 */
trait Expression extends Instruction {
  var alias: Option[String] = None

  def as(name: String): this.type = as(name = Option(name))

  def as(name: Option[String]): this.type = {
    this.alias = name
    this
  }

  def isAggregation: Boolean = false

  def isPrimitive: Boolean = false

  override def toSQL: String = alias.map(name => s"${super.wrapSQL} as $name").getOrElse(super.toSQL)

}

/**
 * Expression Companion
 */
object Expression {

  /**
   * Parses the next expression modifier (e.g. "where lastSale <= 1")
   * @param stream     the given [[TokenStream token stream]]
   * @param expression the source [[Expression expression]]
   * @return a new queryable having the specified modification
   * @example @stocks where lastSale <= 1 order by symbol limit 5
   */
  @tailrec
  def apply(stream: TokenStream, expression: Expression)(implicit compiler: SQLCompiler): Expression = {
    val t0 = stream.peek
    val newExpression: Expression = {
      compiler.ctx.getExpressionChain(stream, expression) getOrElse {
        expression match {
          case queryable: Queryable => Queryable(stream, queryable)
          case expression: Expression => compiler.nextExpression(stream, expr0 = Option(expression)) getOrElse expression
          case other => other
        }
      }.tag(t0)
    }

    // if we got back the same instruction, drop out.
    // otherwise, try to consume another
    if (expression == newExpression) compiler.nextExpression(stream, expr0 = Option(expression)) getOrElse expression
    else Expression(stream, newExpression)
  }

}
