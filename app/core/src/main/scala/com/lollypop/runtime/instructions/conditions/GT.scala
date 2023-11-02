package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * SQL: `a` is greater than `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class GT(a: Expression, b: Expression) extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val result = Option(LollypopVM.execute(scope, a)._3) > Option(LollypopVM.execute(scope, b)._3)
    (scope, IOCost.empty, result)
  }

  override def operator: String = ">"

}

object GT extends ExpressionToConditionPostParser {

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[GT] = {
    if (ts nextIf ">") compiler.nextExpression(ts).map(GT(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is ">"

}
