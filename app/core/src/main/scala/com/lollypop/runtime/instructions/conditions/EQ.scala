package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.EQ.keyword

/**
 * SQL: `a` is equal to `b`
 * @param a the left-side [[Expression expression]]
 * @param b the right-side [[Expression expression]]
 */
case class EQ(a: Expression, b: Expression) extends AbstractEquals {

  override def operator: String = keyword

}

object EQ extends ExpressionToConditionPostParser {
  private val keyword = "=="

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` == `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 200
         |x == 200
         |""".stripMargin
  ), HelpDoc(
    name = "is",
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = "`value` == `expression`",
    description = "returns true if the `value` is exactly the `expression`; otherwise false",
    example =
      """|x = 201
         |x == 200
         |""".stripMargin
  ))

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[EQ] = {
    if (ts nextIf keyword) compiler.nextExpression(ts).map(EQ(host, _)) else None
  }

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}