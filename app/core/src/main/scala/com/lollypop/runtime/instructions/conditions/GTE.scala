package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.GTE.keyword
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * SQL: `a` is greater than or equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class GTE(a: Expression, b: Expression) extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = b match {
    // x >= y < z
    case LT(value, z) => Betwixt(value, a, z).execute()
    // x >= y <= z
    case LTE(value, z) => Between(value, a, z).execute()
    // x >= y
    case _ =>
      val (sa, ca, va) = a.execute(scope)
      val (sb, cb, vb) = b.execute(sa)
      (sb, ca ++ cb, Option(va) >= Option(vb))
  }

  override def operator: String = keyword

}

object GTE extends ExpressionToConditionPostParser {
  private val keyword = ">="

  override def help: List[HelpDoc] = Nil

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[GTE] = {
    if (ts nextIf keyword) compiler.nextExpression(ts).map(GTE(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}