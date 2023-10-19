package com.qwery.runtime.instructions.conditions

import com.qwery.implicits.MagicImplicits
import com.qwery.language.HelpDoc.{CATEGORY_DATAFRAME, PARADIGM_IMPERATIVE}
import com.qwery.language.models.{ArrayExpression, Expression, Instruction, Queryable}
import com.qwery.language.{ExpressionToConditionPostParser, HelpDoc, SQLCompiler, TokenStream, dieUnsupportedEntity}
import com.qwery.runtime.instructions.expressions.{ArrayFromRange, ArrayLiteral}
import com.qwery.runtime.{QweryVM, Scope}
import com.qwery.util.OptionHelper.OptionEnrichment

/**
 * Creates an IN clause
 * @param expr   the given field/expression for which to compare
 * @param source the source of the comparison
 * @author lawrence.daniels@gmail.com
 */
case class IN(expr: Expression, source: Instruction) extends RuntimeCondition {
  override def isTrue(implicit scope: Scope): Boolean = {
    source match {
      case array: ArrayFromRange.Exclusive => Betwixt(expr, array.start, array.end).isTrue
      case array: ArrayFromRange.Inclusive => Between(expr, array.start, array.end).isTrue
      case array: ArrayLiteral => (QweryVM.execute(scope, expr)._3, array.value.map(QweryVM.execute(scope, _)._3)) ~> { case (value, rows) => rows.contains(value) }
      case source: Queryable =>
        val (scope1, cost1, value) = QweryVM.execute(scope, expr)
        val (_, _, device) = QweryVM.search(scope1, source)
        device.toList.exists(_.getField(0).value == value)
      case unknown => dieUnsupportedEntity(unknown, entityName = "queryable")
    }
  }

  override def toSQL: String = s"${expr.toSQL} in ${source.toSQL}"
}

object IN extends ExpressionToConditionPostParser {
  private val __name = "in"

  override def parseConditionChain(ts: TokenStream, host: Expression)(implicit compiler: SQLCompiler): Option[IN] = {
    if (ts.nextIf(__name)) {
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
    name = __name,
    category = CATEGORY_DATAFRAME,
    paradigm = PARADIGM_IMPERATIVE,
    syntax = s"`value` ${__name} `expression`",
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
         |@@stocks where market in ["NASDAQ", "OTCBB"]
         |""".stripMargin
  ))

  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = ts is __name

}