package com.lollypop.runtime.instructions.conditions

import com.lollypop.language.HelpDoc.{CATEGORY_FILTER_MATCH_OPS, PARADIGM_DECLARATIVE}
import com.lollypop.language._
import com.lollypop.language.models.{ArrayExpression, Expression, Instruction, Queryable}
import com.lollypop.runtime.instructions.expressions.{ArrayFromRange, ArrayLiteral}
import com.lollypop.runtime.{Scope, _}
import lollypop.io.IOCost

/**
 * Creates an IN clause
 * @param expr   the given field/expression for which to compare
 * @param source the source of the comparison
 * @author lawrence.daniels@gmail.com
 */
case class IN(expr: Expression, source: Instruction) extends RuntimeCondition {
  override def execute()(implicit scope: Scope): (Scope, IOCost, Boolean) = {
    source match {
      case a: ArrayFromRange.Exclusive => Betwixt(expr, a.start, a.end).execute()
      case a: ArrayFromRange.Inclusive => Between(expr, a.start, a.end).execute()
      case a: ArrayLiteral =>
        val (s1, c1, value) = expr.execute(scope)
        val (s2, c2, rows) = a.value.transform(s1)
        (s2, c1 ++ c2, rows.contains(value))
      case q: Queryable =>
        val (s1, c1, value) = expr.execute(scope)
        val (s2, c2, device) = q.search(s1)
        (s2, c1 ++ c2, device.exists(_.getField(0).value == value))
      case unknown => dieUnsupportedEntity(unknown, entityName = "queryable")
    }
  }

  override def toSQL: String = Seq(expr.toSQL, "in", source.toSQL).mkString(" ")
}

object IN extends ExpressionToConditionPostParser {
  private val keyword = "in"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[IN] = {
    if (ts.nextIf(keyword)) {
      Option(host) map { expr =>
        ts match {
          case ts if ts is "(" => IN(expr, compiler.nextQueryOrVariableWithAlias(ts))
          case ts if ts is "[" => IN(expr, ArrayExpression.parseExpression(ts) || ts.dieExpectedArray())
          case ts => IN(expr, compiler.nextQueryOrVariableWithAlias(ts))
        }
      }
    } else None
  }

  override def help: List[HelpDoc] = List(HelpDoc(
    name = keyword,
    category = CATEGORY_FILTER_MATCH_OPS,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = s"`value` $keyword `expression`",
    description = "determines whether the `value` matches the `expression`",
    example =
      """|val stocks = (
         ||-------------------------------------------------------------------------|
         || ticker | market | lastSale | roundedLastSale | lastSaleTime             |
         ||-------------------------------------------------------------------------|
         || AQKU   | NASDAQ |  68.2945 |            68.2 | 2022-09-04T23:36:47.860Z |
         || WRGB   | AMEX   |  46.8355 |            46.8 | 2022-09-04T23:36:47.862Z |
         || NKWI   | OTCBB  |  98.9501 |            98.9 | 2022-09-04T23:36:47.846Z |
         || ESCN   | AMEX   |  42.5934 |            42.5 | 2022-09-04T23:36:47.865Z |
         || NFRK   | AMEX   |  28.2808 |            28.2 | 2022-09-04T23:36:47.864Z |
         ||-------------------------------------------------------------------------|
         |)
         |@stocks where market in ["NASDAQ", "OTCBB"]
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is keyword

}