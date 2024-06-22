package com.lollypop.runtime.instructions.functions

import com.lollypop.language.HelpDoc.{CATEGORY_MACHINE_LEARNING, PARADIGM_DECLARATIVE}
import com.lollypop.language.models.Expression
import com.lollypop.language.{ExpressionParser, HelpDoc, SQLCompiler, SQLTemplateParams, TokenStream}
import com.lollypop.runtime.instructions.expressions.RuntimeExpression
import com.lollypop.runtime.instructions.functions.Histogram.{computeIndex, computeRanges}
import com.lollypop.runtime.{ExpressiveTypeConversion, Scope}
import lollypop.io.IOCost

/**
 * Histogram Function
 * @example Histogram(lastSale, [0, 1, 5, 10, 20, 100, 250, 1000])
 * @param columnName the column whose value will be substituted with the histogram index
 * @param points     the [[Expression array]] describing the points of the histogram
 */
case class Histogram(columnName: Expression, points: Expression)
  extends DecompositionFunction with RuntimeExpression {

  override def execute()(implicit scope: Scope): (Scope, IOCost, Option[Int]) = {
    val (_, ca, myPoints) = points.pullArray(scope).map(_ map {
      case n: Double => n
      case n: Number => n.doubleValue()
      case z => dieIllegalType(z)
    })
    val myName = getColumnName
    val myRange = computeRanges(myPoints)
    val myValue = scope.resolve(myName).map {
        case d: Double => d
        case n: Number => n.doubleValue()
        case z => dieIllegalType(z)
      }
      .getOrElse(dieExpectedExpression())
    (scope, ca, computeIndex(myRange, myValue))
  }

  override def toSQL: String = s"Histogram(${columnName.toSQL}, ${points.toSQL})"

}

/**
 * Histogram Companion
 */
object Histogram extends ExpressionParser {
  private val templateCard = "Histogram ( %e:column , %e:points )"

  def computeRanges(points: Array[Double]): List[(Double, Double)] = {
    points.sliding(2, 1).toList.map { case Array(a, b) => (a, b) }
  }

  def computeIndex(ranges: List[(Double, Double)], value: Double): Option[Int] = {
    ranges.indexWhere { case (a, b) => (value >= a) && (value < b) } match {
      case -1 => None
      case n => Some(n)
    }
  }

  /**
   * Provides help for instruction
   * @return the [[HelpDoc help-doc]]
   */
  override def help: List[HelpDoc] = List(HelpDoc(
    name = "Histogram",
    category = CATEGORY_MACHINE_LEARNING,
    paradigm = PARADIGM_DECLARATIVE,
    syntax = templateCard,
    description = "Creates a histogram function for attribution",
    example =
      """|Histogram(last_sale, [0, 1, 5, 10, 20, 100, 250, 1000])
         |""".stripMargin,
    isExperimental = true
  ))

  override def parseExpression(ts: TokenStream)(implicit compiler: SQLCompiler): Option[Histogram] = {
    if (!understands(ts)) None else {
      val params = SQLTemplateParams(ts, templateCard)
      Some(Histogram(
        columnName = params.expressions("column"),
        points = params.expressions("points")))
    }
  }

  /**
   * Indicates whether the next token in the stream cam be parsed
   * @param ts       the [[TokenStream token stream]]
   * @param compiler the [[SQLCompiler compiler]]
   * @return true, if the next token in the stream is parseable
   */
  override def understands(ts: TokenStream)(implicit compiler: SQLCompiler): Boolean = {
    ts is "Histogram"
  }

}