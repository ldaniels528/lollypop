package com.qwery.runtime.instructions.conditions

import com.qwery.language.models.Expression
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.qwery.runtime.{QweryVM, Scope}

/**
 * SQL: `a` is less than `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class LT(a: Expression, b: Expression) extends RuntimeInequality {

  override def isTrue(implicit scope: Scope): Boolean = Option(QweryVM.execute(scope, a)._3) < Option(QweryVM.execute(scope, b)._3)

  override def operator: String = "<"

}

object LT extends ExpressionToConditionPostParser {

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[LT] = {
    if (ts nextIf "<") compiler.nextExpression(ts).map(LT(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is "<"

}