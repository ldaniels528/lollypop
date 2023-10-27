package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{Condition, Expression}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.conditions.Is.__name
import com.qwery.runtime.{QweryVM, Scope}

/**
 * SQL: `a` is equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class Is(a: Expression, b: Expression) extends RuntimeInequality {

  override def isTrue(implicit scope: Scope): Boolean = QweryVM.execute(scope, a)._3 == QweryVM.execute(scope, b)._3

  override def operator: String = __name

}

object Is extends ExpressionToConditionPostParser {
  private val __name = "is"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(__name)) {
      if (ts.nextIf("not null")) Option(IsNotNull(host))
      else if (ts.nextIf("not")) compiler.nextExpression(ts).map(Isnt(host, _))
      else if (ts.nextIf("null")) Option(IsNull(host))
      else compiler.nextExpression(ts).map(Is(host, _))
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "is",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x is 200
         |""".stripMargin
  ), HelpDoc(
    name = "is",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 201
         |x is 200
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}
