package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{Condition, Expression}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.Isnt.keyword
import com.lollypop.runtime.{LollypopVM, Scope}
import lollypop.io.IOCost

/**
 * Isnt: `a` is not equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class Isnt(a: Expression, b: Expression) extends RuntimeInequality {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val result = LollypopVM.execute(scope, a)._3 != LollypopVM.execute(scope, b)._3
    (scope, IOCost.empty, result)
  }

  override def operator: String = keyword

}

object Isnt extends ExpressionToConditionPostParser {
  private val keyword = "isnt"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(keyword)) compiler.nextExpression(ts).map(Isnt(host, _)) else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = "isnt",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` isnt `expression`",
    description = "returns true if the `value` is not exactly the `expression`; otherwise false",
    example =
      """|x = 199
         |x isnt 200
         |""".stripMargin
  ), HelpDoc(
    name = "isnt",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` isnt `expression`",
    description = "returns true if the `value` is not exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x isnt 200
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
