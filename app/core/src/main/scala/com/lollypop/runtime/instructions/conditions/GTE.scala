package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.lollypop.runtime.{LollypopVM, Scope}

/**
 * SQL: `a` is greater than or equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class GTE(a: Expression, b: Expression) extends RuntimeInequality {

  override def isTrue(implicit scope: Scope): Boolean = Option(LollypopVM.execute(scope, a)._3) >= Option(LollypopVM.execute(scope, b)._3)

  override def operator: String = ">="

}

object GTE extends ExpressionToConditionPostParser {

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[GTE] = {
    if (ts nextIf ">=") compiler.nextExpression(ts).map(GTE(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is ">="

}