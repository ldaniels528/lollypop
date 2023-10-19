package com.qwery.language.models

import com.qwery.language.TemplateProcessor.TagInstructionWithLineNumbers
import com.qwery.language.{SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.queryables.AssumeQueryable.EnrichedAssumeQueryable
import com.qwery.util.OptionHelper.OptionEnrichment

/**
  * Represents a queryable instruction
  * @author lawrence.daniels@gmail.com
  */
trait Queryable extends Expression {

  def isChainable: Boolean = true

}

object Queryable {

  def apply(ts: TokenStream)(implicit compiler: SQLCompiler): Queryable = {
    compiler.nextExpression(ts).map(_.asQueryable) || ts.dieExpectedQueryable()
  }

  /**
   * Parses the next query modifier (e.g. "where lastSale <= 1")
   * @param stream    the given [[TokenStream token stream]]
   * @param queryable the source [[Queryable queryable]]
   * @return a new queryable having the specified modification
   * @example @@stocks where lastSale <= 1 order by symbol limit 5
   */
  def apply(stream: TokenStream, queryable: Queryable)(implicit compiler: SQLCompiler): Queryable = {
    val t0 = stream.peek
    compiler.ctx.getQueryableChain(stream, queryable).map(_.tag(t0)) || queryable
  }

}