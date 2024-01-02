package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.{Condition, Expression}
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.Is.keyword

/**
 * SQL: `a` is equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class Is(a: Expression, b: Expression) extends AbstractEquals {

  override def operator: String = keyword

}

object Is extends ExpressionToConditionPostParser {
  private val keyword = "is"

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x is 200
         |""".stripMargin
  ), HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` is `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 201
         |x is 200
         |""".stripMargin
  ))

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Condition] = {
    if (ts.nextIf(keyword)) compiler.nextExpression(ts).map(Is(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
