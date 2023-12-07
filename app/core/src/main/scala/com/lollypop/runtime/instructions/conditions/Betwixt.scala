package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream}
import com.lollypop.runtime.instructions.conditions.Betwixt.keyword
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Betwixt Operator: `value` betwixt `from` and `to`
 * @param value the [[Expression expression]] to evaluate
 * @param from  the lower bound [[Expression expression]]
 * @param to    the upper bound [[Expression expression]]
 */
case class Betwixt(value: Expression, from: Expression, to: Expression) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    val (sa, ca, v) = value.execute(scope)
    val (sb, cb, a) = from.execute(sa)
    val (sc, cc, b) = to.execute(sb)
    val (vv, aa, bb) = (Option(v), Option(a), Option(b))
    (sc, ca ++ cb ++ cc, (vv >= aa) && (vv < bb))
  }

  override def toSQL: String = Seq(value.toSQL, keyword, from.toSQL, "and", to.toSQL).mkString(" ")
}

object Betwixt extends ExpressionToConditionPostParser {
  private val keyword = "betwixt"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[Betwixt] = {
    if (ts.nextIf(keyword)) {
      for {
        a <- compiler.nextExpression(ts)
        _ = ts expect "and"
        b <- compiler.nextExpression(ts)
      } yield Betwixt(host, a, b)
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `to` and `from`",
    description = "determines whether the `value` is between the `to` and `from` (non-inclusive)",
    example =
      """|from (
         ||-------------------------------------------------------------------------|
         || ticker | market | lastSale | roundedLastSale | lastSaleTime             |
         ||-------------------------------------------------------------------------|
         || NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
         || AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
         || WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
         || ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
         || NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
         ||-------------------------------------------------------------------------|
         |) where lastSale betwixt 28.2808 and 42.5934
         |  order by lastSale desc
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}
