package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.LollypopVM.implicits.InstructionExtensions
import com.lollypop.runtime.Scope
import com.lollypop.runtime.instructions.conditions.LTE.keyword
import com.lollypop.runtime.instructions.conditions.RuntimeInequality.OptionComparator
import lollypop.io.IOCost

/**
 * SQL: `a` is less than or equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class LTE(a: Expression, b: Expression) extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, ra) = a.execute(scope)
    val (sb, cb, rb) = b.execute(sa)
    (sb, ca ++ cb, Option(ra) <= Option(rb))
  }

  override def operator: String = keyword

}

object LTE extends ExpressionToConditionPostParser {
  private val keyword = "<="

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[LTE] = {
    if (ts nextIf keyword) compiler.nextExpression(ts).map(LTE(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}