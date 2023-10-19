package com.qwery.runtime.instructions.conditions

import com.qwery.language.HelpDoc.{CATEGORY_BRANCHING_OPS, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{Condition, Expression}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.qwery.runtime.instructions.conditions.Isnt.__name
import com.qwery.runtime.{QweryVM, Scope}

/**
 * Isnt: `a` is not equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class Isnt(a: Expression, b: Expression) extends RuntimeInequality {

  override def isTrue(implicit scope: Scope): Boolean = QweryVM.execute(scope, a)._3 != QweryVM.execute(scope, b)._3

  override def operator: String = __name

}

object Isnt extends ExpressionToConditionPostParser {
  private val __name = "isnt"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(__name)) compiler.nextExpression(ts).map(Isnt(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "isnt",
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = "`value` isnt `expression`",
    description = "returns true if the `value` is not exactly the `expression`; otherwise false",
    example =
      """|x = 199
         |x isnt 200
         |""".stripMargin
  ), HelpDoc(
    name = "isnt",
    category = CATEGORY_BRANCHING_OPS,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = "`value` isnt `expression`",
    description = "returns true if the `value` is not exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x isnt 200
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}
