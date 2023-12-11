package com.lollypop.language.models

import com.lollypop.language._

/**
 * Represents a queryable instruction
 */
trait Queryable extends Expression

/**
 * Queryable Companion
 */
object Queryable {

  /**
   * Parses the next queryable (e.g. "select n: 5")
   * @param ts the given [[TokenStream token stream]]
   * @return a new [[Queryable queryable]]
   * @example select n: 5
   */
  def apply(ts: TokenStream)(implicit compiler: SQLCompiler): Queryable = {
    compiler.nextExpression(ts).map(_.asQueryable) || ts.dieExpectedQueryable()
  }

  /**
   * Parses the next query modifier (e.g. "where lastSale <= 1")
   * @param ts        the given [[TokenStream token stream]]
   * @param queryable the source [[Queryable queryable]]
   * @return a new [[Queryable queryable]] having the specified modification
   * @example @stocks where lastSale <= 1 order by symbol limit 5
   */
  def apply(ts: TokenStream, queryable: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val t0 = ts.peek
    compiler.ctx.getQueryableChain(ts, queryable).map(_.tag(t0)) || queryable
  }

}